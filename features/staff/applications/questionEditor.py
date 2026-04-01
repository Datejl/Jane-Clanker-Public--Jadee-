from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

import discord

from runtime import interaction as interactionRuntime
from runtime import viewBases as runtimeViewBases

if TYPE_CHECKING:
    from cogs.applicationsCog import ApplicationsCog


def _normalizeQuestionStyle(rawStyle: Any) -> str:
    style = str(rawStyle or "").strip().lower()
    if style in {"short", "paragraph", "form", "multiple-choice", "multiplechoice", "choice", "select"}:
        if style in {"multiplechoice", "choice", "select"}:
            return "multiple-choice"
        return style
    if style in {"server-invite", "discord-server", "server", "discord-invite", "invite"}:
        return "server-invite"
    return "short"


def _questionStyleLabel(style: str) -> str:
    normalized = _normalizeQuestionStyle(style)
    if normalized == "paragraph":
        return "Paragraph"
    if normalized == "multiple-choice":
        return "Multiple Choice"
    if normalized == "form":
        return "Form Link"
    if normalized == "server-invite":
        return "Server Invite"
    return "Short Response"


def _questionTypeKeyToken(style: str) -> str:
    normalized = _normalizeQuestionStyle(style)
    if normalized == "paragraph":
        return "paragraph"
    if normalized == "multiple-choice":
        return "multiplechoice"
    if normalized == "form":
        return "form"
    if normalized == "server-invite":
        return "serverinvite"
    return "short"


def _shouldNumberQuestionType(style: str) -> bool:
    normalized = _normalizeQuestionStyle(style)
    return normalized not in {"form", "server-invite"}


class ApplicationsDivisionQuestionSelect(discord.ui.Select):
    def __init__(self, viewRef: "ApplicationsDivisionQuestionsView", options: list[discord.SelectOption]):
        self.viewRef = viewRef
        super().__init__(
            placeholder="Select a question",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        try:
            selectedIndex = int((self.values or ["0"])[0])
        except (TypeError, ValueError):
            selectedIndex = 0
        self.viewRef.selectedIndex = max(0, selectedIndex)
        await self.viewRef.refresh(interaction)


class ApplicationsDivisionQuestionTypeSelect(discord.ui.Select):
    def __init__(self, cog: "ApplicationsCog", divisionKey: str, questionIndex: int):
        self.cog = cog
        self.divisionKey = divisionKey
        self.questionIndex = int(questionIndex)
        options = [
            discord.SelectOption(label="Short Response", value="short"),
            discord.SelectOption(label="Paragraph", value="paragraph"),
            discord.SelectOption(label="Multiple Choice", value="multiple-choice"),
            discord.SelectOption(label="Form Link", value="form"),
            discord.SelectOption(label="Server Invite", value="server-invite"),
        ]
        super().__init__(
            placeholder="Choose question type",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        selectedStyle = str((self.values or ["short"])[0])
        await self.cog.updateDivisionQuestionStyle(
            interaction,
            divisionKey=self.divisionKey,
            questionIndex=self.questionIndex,
            style=selectedStyle,
        )


class ApplicationsDivisionQuestionTypeView(discord.ui.View):
    def __init__(self, cog: "ApplicationsCog", divisionKey: str, questionIndex: int):
        super().__init__(timeout=300)
        self.add_item(ApplicationsDivisionQuestionTypeSelect(cog, divisionKey, questionIndex))


class ApplicationsDivisionTextQuestionModal(discord.ui.Modal):
    labelInput = discord.ui.TextInput(
        label="Question Label",
        required=True,
        max_length=100,
    )
    placeholderInput = discord.ui.TextInput(
        label="Placeholder (optional)",
        required=False,
        max_length=100,
    )
    maxLengthInput = discord.ui.TextInput(
        label="Max Length (1-4000)",
        required=False,
        max_length=8,
        placeholder="400",
    )
    requiredInput = discord.ui.TextInput(
        label="Required (true/false)",
        required=False,
        max_length=8,
        placeholder="true",
    )

    def __init__(
        self,
        cog: "ApplicationsCog",
        divisionKey: str,
        questionIndex: Optional[int],
        question: Optional[dict[str, Any]] = None,
    ):
        title = "Add Question" if questionIndex is None else "Edit Text Question"
        super().__init__(title=title)
        self.cog = cog
        self.divisionKey = divisionKey
        self.questionIndex = questionIndex
        question = question or {}
        self.labelInput.default = str(question.get("label") or "")
        self.placeholderInput.default = str(question.get("placeholder") or "")
        self.maxLengthInput.default = str(question.get("maxLength") or "400")
        self.requiredInput.default = "true" if bool(question.get("required", True)) else "false"

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.upsertDivisionTextQuestion(
            interaction,
            divisionKey=self.divisionKey,
            questionIndex=self.questionIndex,
            label=str(self.labelInput.value or "").strip(),
            placeholder=str(self.placeholderInput.value or "").strip(),
            maxLengthRaw=str(self.maxLengthInput.value or "").strip(),
            requiredRaw=str(self.requiredInput.value or "").strip(),
        )


class ApplicationsDivisionLinkQuestionModal(discord.ui.Modal):
    labelInput = discord.ui.TextInput(
        label="Question Label",
        required=True,
        max_length=100,
    )
    urlInput = discord.ui.TextInput(
        label="URL",
        required=True,
        max_length=500,
    )

    def __init__(
        self,
        cog: "ApplicationsCog",
        divisionKey: str,
        questionIndex: int,
        question: dict[str, Any],
    ):
        style = _normalizeQuestionStyle(question.get("style"))
        title = "Edit Form Link" if style == "form" else "Edit Server Invite Link"
        super().__init__(title=title)
        self.cog = cog
        self.divisionKey = divisionKey
        self.questionIndex = int(questionIndex)
        self.style = style
        self.labelInput.default = str(question.get("label") or "")
        if style == "form":
            self.urlInput.default = str(question.get("link") or question.get("url") or "")
        else:
            self.urlInput.default = str(question.get("inviteUrl") or question.get("url") or "")

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.updateDivisionLinkQuestion(
            interaction,
            divisionKey=self.divisionKey,
            questionIndex=self.questionIndex,
            style=self.style,
            label=str(self.labelInput.value or "").strip(),
            url=str(self.urlInput.value or "").strip(),
        )


class ApplicationsDivisionChoiceQuestionModal(discord.ui.Modal, title="Edit Multiple Choice Question"):
    labelInput = discord.ui.TextInput(
        label="Question Label",
        required=True,
        max_length=100,
    )
    choicesInput = discord.ui.TextInput(
        label="Choices (one per line)",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000,
    )
    requiredInput = discord.ui.TextInput(
        label="Required (true/false)",
        required=False,
        max_length=8,
        placeholder="true",
    )

    def __init__(
        self,
        cog: "ApplicationsCog",
        divisionKey: str,
        questionIndex: int,
        question: dict[str, Any],
    ):
        super().__init__()
        self.cog = cog
        self.divisionKey = divisionKey
        self.questionIndex = int(questionIndex)
        self.labelInput.default = str(question.get("label") or "")
        rawChoices = question.get("choices")
        if isinstance(rawChoices, list):
            self.choicesInput.default = "\n".join(str(choice).strip() for choice in rawChoices if str(choice).strip())
        else:
            self.choicesInput.default = ""
        self.requiredInput.default = "true" if bool(question.get("required", True)) else "false"

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog.updateDivisionChoiceQuestion(
            interaction,
            divisionKey=self.divisionKey,
            questionIndex=self.questionIndex,
            label=str(self.labelInput.value or "").strip(),
            choicesRaw=str(self.choicesInput.value or "").strip(),
            requiredRaw=str(self.requiredInput.value or "").strip(),
        )


class ApplicationsDivisionQuestionsView(discord.ui.View):
    def __init__(self, cog: "ApplicationsCog", divisionKey: str, selectedIndex: int = 0):
        super().__init__(timeout=600)
        self.cog = cog
        self.divisionKey = divisionKey
        self.selectedIndex = max(0, int(selectedIndex))
        self._refreshQuestionSelect()

    def _buildQuestionOptions(self) -> list[discord.SelectOption]:
        questions = self.cog.getDivisionQuestionsForEditor(self.divisionKey)
        if not questions:
            return [discord.SelectOption(label="No questions configured", value="0")]
        options: list[discord.SelectOption] = []
        for index, question in enumerate(questions):
            label = str(question.get("label") or f"Question {index + 1}")
            styleLabel = _questionStyleLabel(str(question.get("style") or "short"))
            options.append(
                discord.SelectOption(
                    label=f"{index + 1}. {label}"[:100],
                    description=styleLabel[:100],
                    value=str(index),
                    default=(index == self.selectedIndex),
                )
            )
        return options

    def _refreshQuestionSelect(self) -> None:
        for child in list(self.children):
            if isinstance(child, ApplicationsDivisionQuestionSelect):
                self.remove_item(child)
        self.add_item(ApplicationsDivisionQuestionSelect(self, self._buildQuestionOptions()))

    async def refresh(self, interaction: discord.Interaction) -> None:
        embed = self.cog.buildDivisionQuestionsEmbed(self.divisionKey, self.selectedIndex)
        self._refreshQuestionSelect()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=embed,
            view=self,
        )

    @discord.ui.button(label="Add Question", style=discord.ButtonStyle.success, row=1)
    async def addBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            ApplicationsDivisionTextQuestionModal(self.cog, self.divisionKey, questionIndex=None),
        )

    @discord.ui.button(label="Edit Selected", style=discord.ButtonStyle.primary, row=1)
    async def editBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.editSelectedDivisionQuestion(
            interaction,
            divisionKey=self.divisionKey,
            questionIndex=self.selectedIndex,
        )

    @discord.ui.button(label="Remove Selected", style=discord.ButtonStyle.danger, row=1)
    async def removeBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.removeDivisionQuestion(
            interaction,
            divisionKey=self.divisionKey,
            questionIndex=self.selectedIndex,
        )
        await self.refresh(interaction)

    @discord.ui.button(label="Change Type", style=discord.ButtonStyle.secondary, row=2)
    async def typeBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        questions = self.cog.getDivisionQuestionsForEditor(self.divisionKey)
        if not questions:
            await interactionRuntime.safeInteractionReply(
                interaction,
                content="No questions to edit.",
                ephemeral=True,
            )
            return
        safeIndex = min(max(self.selectedIndex, 0), len(questions) - 1)
        await interactionRuntime.safeInteractionReply(
            interaction,
            content="Pick a type for the selected question.",
            view=ApplicationsDivisionQuestionTypeView(self.cog, self.divisionKey, safeIndex),
            ephemeral=True,
        )

    @discord.ui.button(label="Toggle Required", style=discord.ButtonStyle.secondary, row=2)
    async def requiredBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.toggleDivisionQuestionRequired(
            interaction,
            divisionKey=self.divisionKey,
            questionIndex=self.selectedIndex,
        )
        await self.refresh(interaction)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=2)
    async def refreshBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.refresh(interaction)


__all__ = [
    "ApplicationsDivisionChoiceQuestionModal",
    "ApplicationsDivisionLinkQuestionModal",
    "ApplicationsDivisionQuestionsView",
    "ApplicationsDivisionQuestionTypeView",
    "ApplicationsDivisionTextQuestionModal",
    "_normalizeQuestionStyle",
    "_questionStyleLabel",
    "_questionTypeKeyToken",
    "_shouldNumberQuestionType",
]
