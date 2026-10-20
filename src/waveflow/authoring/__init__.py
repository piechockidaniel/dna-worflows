"""Collaborative authoring (plan phase P11)."""

from DnA Workflows.authoring.drafts import Draft, DraftConflict, DraftNotFound, DraftService, DraftStateError, DraftStatus
from DnA Workflows.authoring.testrun import TestRunner, TestRunRequest
from DnA Workflows.kernel.clock import Clock
from DnA Workflows.kernel.container import ServiceCollection
from DnA Workflows.kernel.features import Feature, FeatureOptions
from DnA Workflows.kernel.ids import IdGenerator
from DnA Workflows.persistence.contracts import DefinitionStore, JournalStore, KeyValueStore
from DnA Workflows.runtime.activity import ActivityRegistry
from DnA Workflows.runtime.contracts import FaultClassifier
from DnA Workflows.runtime.registry import WorkflowRegistry


class AuthoringOptions(FeatureOptions):
    #: Publishing requires an approved review.
    review_required: bool = True
    #: The last editor cannot approve their own changes.
    four_eyes: bool = True


class AuthoringFeature(Feature):
    """Drafts, reviews, publishing and test runs for collaborative, analyst-driven design."""

    name = "authoring"
    depends_on = ("runtime",)
    Options = AuthoringOptions

    def configure(self, services: ServiceCollection) -> None:
        o: AuthoringOptions = self.options  # type: ignore[assignment]
        services.add(DraftService, lambda p: DraftService(
            p.get(KeyValueStore), p.get(WorkflowRegistry), p.get(DefinitionStore), p.get(JournalStore),
            p.get(Clock), p.get(IdGenerator), review_required=o.review_required, four_eyes=o.four_eyes))
        services.add(TestRunner, lambda p: TestRunner(p, p.get(ActivityRegistry), p.get(Clock),
                                                      p.get_all(FaultClassifier)))


__all__ = ["AuthoringFeature", "Draft", "DraftConflict", "DraftNotFound", "DraftService", "DraftStateError",
           "DraftStatus", "TestRunRequest", "TestRunner"]
