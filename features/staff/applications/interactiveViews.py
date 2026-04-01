from __future__ import annotations

from typing import Any

import discord

from runtime import interaction as interactionRuntime
from runtime import viewBases as runtimeViewBases


def _choiceQuestionTitle(question: dict[str, Any], *, fallback: str = "Question") -> str:
    label = str(question.get("label") or "").strip()
    key = str(question.get("key") or "").strip()
    if label and key and label.lower().startswith(key.lower()):
        stripped = label[len(key):].lstrip(" .:-)")
        if stripped:
            return stripped
    return label or fallback


class DivisionApplyModal(discord.ui.Modal):
    def __init__(
        self,
        cog: Any,
        division: dict[str, Any],
        textQuestions: list[dict[str, Any]],
        remainingQuestions: list[dict[str, Any]] | None = None,
        answersSoFar: dict[str, str] | None = None,
    ):
        super().__init__(title=f"Apply - {division['displayName']}"[:45], timeout=600)
        self.cog = cog
        self.divisionKey = division["key"]
        self.remainingQuestions = list(remainingQuestions or [])
        self.answersSoFar = dict(answersSoFar or {})
        self.questionInputs: list[tuple[dict[str, Any], discord.ui.TextInput]] = []
        for question in textQuestions:
            style = discord.TextStyle.paragraph if question["style"] == "paragraph" else discord.TextStyle.short
            textInput = discord.ui.TextInput(
                label=str(question["label"])[:45],
                required=bool(question["required"]),
                max_length=max(1, min(int(question["maxLength"]), 4000)),
                style=style,
                placeholder=question.get("placeholder") or None,
            )
            self.add_item(textInput)
            self.questionInputs.append((question, textInput))

    async def on_submit(self, interaction: discord.Interaction) -> None:
        answers: dict[str, str] = dict(self.answersSoFar)
        for question, textInput in self.questionInputs:
            label = str(question.get("label") or question.get("key") or "Question")
            answers[label] = str(textInput.value or "").strip()
        if self.remainingQuestions:
            await self.cog.openApplicationQuestionStep(
                interaction,
                divisionKey=self.divisionKey,
                answers=answers,
                remainingQuestions=self.remainingQuestions,
            )
            return
        await self.cog.handleModalSubmit(interaction, self.divisionKey, answers)


class DivisionChoiceOptionSelect(discord.ui.Select):
    def __init__(self, viewRef: "DivisionMultipleChoiceView"):
        self.viewRef = viewRef
        question = viewRef.currentQuestion()
        choices = question.get("choices") if isinstance(question.get("choices"), list) else []
        currentValue = viewRef.selections.get(viewRef.currentIndex, "")
        options: list[discord.SelectOption] = []
        for choice in choices[:25]:
            cleanChoice = str(choice or "").strip()
            if not cleanChoice:
                continue
            options.append(
                discord.SelectOption(
                    label=cleanChoice[:100],
                    value=cleanChoice,
                    default=(cleanChoice == currentValue),
                )
            )
        if not options:
            options.append(discord.SelectOption(label="No options configured", value=""))
        super().__init__(
            placeholder="Choose an option",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
            disabled=(len(options) == 1 and options[0].value == ""),
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        selectedValue = str((self.values or [""])[0]).strip()
        if selectedValue:
            self.viewRef.selections[self.viewRef.currentIndex] = selectedValue
        else:
            self.viewRef.selections.pop(self.viewRef.currentIndex, None)
        await self.viewRef.refresh(interaction)


class DivisionMultipleChoiceView(runtimeViewBases.OwnerLockedView):
    def __init__(
        self,
        cog: Any,
        divisionKey: str,
        applicantId: int,
        answersSoFar: dict[str, str],
        choiceQuestions: list[dict[str, Any]],
        remainingQuestions: list[dict[str, Any]] | None = None,
    ):
        super().__init__(
            openerId=applicantId,
            timeout=900,
            ownerMessage="Only the applicant can answer these questions.",
        )
        self.cog = cog
        self.divisionKey = divisionKey
        self.applicantId = int(applicantId)
        self.answersSoFar = dict(answersSoFar)
        self.choiceQuestions = list(choiceQuestions)
        self.remainingQuestions = list(remainingQuestions or [])
        self.currentIndex = 0
        self.selections: dict[int, str] = {}
        self._refreshSelect()

    def currentQuestion(self) -> dict[str, Any]:
        if not self.choiceQuestions:
            return {}
        safeIndex = min(max(self.currentIndex, 0), len(self.choiceQuestions) - 1)
        self.currentIndex = safeIndex
        return self.choiceQuestions[safeIndex]

    def _refreshSelect(self) -> None:
        for child in list(self.children):
            if isinstance(child, DivisionChoiceOptionSelect):
                self.remove_item(child)
        if self.choiceQuestions:
            self.add_item(DivisionChoiceOptionSelect(self))
        self.prevBtn.disabled = self.currentIndex <= 0
        self.nextBtn.disabled = self.currentIndex >= len(self.choiceQuestions) - 1

    def buildEmbed(self) -> discord.Embed:
        question = self.currentQuestion()
        total = len(self.choiceQuestions)
        styleLabel = "Required" if bool(question.get("required", True)) else "Optional"
        questionTitle = _choiceQuestionTitle(question)
        selected = self.selections.get(self.currentIndex)
        statusLines: list[str] = []
        for idx, item in enumerate(self.choiceQuestions):
            selectedText = self.selections.get(idx)
            prefix = "->" if idx == self.currentIndex else "  "
            line = f"{prefix} {_choiceQuestionTitle(item, fallback=f'Question {idx + 1}')}"
            if selectedText:
                line = f"{line} [selected]"
            statusLines.append(line)
        embed = discord.Embed(
            title="Application - Multiple Choice",
            description="\n".join(statusLines[:25]),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name=f"{styleLabel} Question",
            value=questionTitle,
            inline=False,
        )
        embed.add_field(
            name="Current Selection",
            value=selected or "(none)",
            inline=False,
        )
        embed.set_footer(text="Use Prev/Next to navigate, then Submit Application.")
        return embed

    async def refresh(self, interaction: discord.Interaction) -> None:
        self._refreshSelect()
        embed = self.buildEmbed()
        await runtimeViewBases.safeRefreshInteractionMessage(
            interaction,
            embed=embed,
            view=self,
        )

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary, row=1)
    async def prevBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.currentIndex > 0:
            self.currentIndex -= 1
        await self.refresh(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary, row=1)
    async def nextBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.currentIndex < len(self.choiceQuestions) - 1:
            self.currentIndex += 1
        await self.refresh(interaction)

    @discord.ui.button(label="Submit Application", style=discord.ButtonStyle.success, row=1)
    async def submitBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        missingRequired: list[str] = []
        for index, question in enumerate(self.choiceQuestions):
            if bool(question.get("required", True)) and not self.selections.get(index):
                missingRequired.append(_choiceQuestionTitle(question, fallback=f"Question {index + 1}"))
        if missingRequired:
            return await interactionRuntime.safeInteractionReply(
                interaction,
                content=f"Please answer required question(s): {', '.join(missingRequired[:3])}",
                ephemeral=True,
            )

        answers = dict(self.answersSoFar)
        for index, question in enumerate(self.choiceQuestions):
            label = str(question.get("label") or question.get("key") or f"Question {index + 1}")
            selectedValue = self.selections.get(index, "").strip()
            if selectedValue:
                answers[label] = selectedValue
            elif bool(question.get("required", True)):
                answers[label] = "(not answered)"
        if self.remainingQuestions:
            await self.cog.openApplicationQuestionStep(
                interaction,
                divisionKey=self.divisionKey,
                answers=answers,
                remainingQuestions=self.remainingQuestions,
            )
            return
        await self.cog.handleModalSubmit(interaction, self.divisionKey, answers)


class NeedsInfoModal(discord.ui.Modal, title="Needs Clarification"):
    noteInput = discord.ui.TextInput(
        label="What needs clarification?",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=1000,
    )

    def __init__(self, cog: Any, applicationId: int):
        super().__init__()
        self.cog = cog
        self.applicationId = int(applicationId)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        note = str(self.noteInput.value or "").strip() or "Please provide additional information and re-apply."
        await self.cog.handleReviewDecision(interaction, self.applicationId, "NEEDS_INFO", note)


class ApplicantAnswerModal(discord.ui.Modal, title="Answer Clarification"):
    answerInput = discord.ui.TextInput(
        label="Your response",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=2000,
    )

    def __init__(self, cog: Any, applicationId: int, promptTitle: str):
        super().__init__()
        self.cog = cog
        self.applicationId = int(applicationId)
        self.promptTitle = (promptTitle or "Clarification").strip()

    async def on_submit(self, interaction: discord.Interaction) -> None:
        answer = str(self.answerInput.value or "").strip()
        await self.cog.handleApplicantAnswerSubmit(
            interaction,
            self.applicationId,
            self.promptTitle,
            answer,
        )


class DivisionHubView(discord.ui.View):
    def __init__(self, cog: Any, divisionKey: str, isOpen: bool = True):
        super().__init__(timeout=None)
        self.cog = cog
        self.divisionKey = divisionKey
        self.isOpen = bool(isOpen)
        self.applyBtn.custom_id = f"apps:apply:{divisionKey}"
        if not self.isOpen:
            self.applyBtn.label = "Applications Closed"
            self.applyBtn.style = discord.ButtonStyle.secondary
            self.applyBtn.disabled = True

    @discord.ui.button(label="Apply", style=discord.ButtonStyle.primary, custom_id="apps:apply:placeholder")
    async def applyBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not self.isOpen:
            return await self.cog.safeReply(interaction, "Applications are currently closed for this division.")
        await self.cog.handleApply(interaction, self.divisionKey)


class DivisionReviewView(discord.ui.View):
    def __init__(self, cog: Any, applicationId: int, status: str = "PENDING", applicantId: int = 0):
        super().__init__(timeout=None)
        self.cog = cog
        self.applicationId = int(applicationId)
        self.status = str(status or "PENDING").upper()
        self.applicantId = int(applicantId or 0)
        self.approveBtn.custom_id = f"apps:approve:{applicationId}"
        self.denyBtn.custom_id = f"apps:deny:{applicationId}"
        self.needsInfoBtn.custom_id = f"apps:needsinfo:{applicationId}"
        if self.status == "NEEDS_INFO":
            self.needsInfoBtn.label = "Answer"
            self.needsInfoBtn.style = discord.ButtonStyle.primary

    def disableAll(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, custom_id="apps:approve:placeholder")
    async def approveBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.handleReviewDecision(interaction, self.applicationId, "APPROVED", None)

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, custom_id="apps:deny:placeholder")
    async def denyBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog.handleReviewDecision(interaction, self.applicationId, "DENIED", None)

    @discord.ui.button(label="Needs Info", style=discord.ButtonStyle.secondary, custom_id="apps:needsinfo:placeholder")
    async def needsInfoBtn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if self.status == "NEEDS_INFO":
            await self.cog.handleApplicantAnswerButton(interaction, self.applicationId, self.applicantId)
            return
        await interactionRuntime.safeInteractionSendModal(
            interaction,
            NeedsInfoModal(self.cog, self.applicationId),
        )
