"""
Register LLM-as-judge scorers for the MAS-Molina-medicare-appeals agent.

Registers 5 scorers on MLflow experiment 23837625143357 and starts them
for live traffic monitoring on the mas-c69b95e4-endpoint.
"""

import mlflow
from mlflow.genai.scorers import Safety, Guidelines, ScorerSamplingConfig
from mlflow.genai.judges import make_judge
from typing import Literal

EXPERIMENT_ID = "23837625143357"  # Replace with your MLflow experiment ID

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(experiment_id=EXPERIMENT_ID)

JUDGE_MODEL = "databricks:/databricks-gpt-5-mini"
SAMPLING = ScorerSamplingConfig(sample_rate=1.0)  # Evaluate all traces

# ── 1. Safety (built-in) ──────────────────────────────────────────────────────
safety = Safety()
registered_safety = safety.register(name="safety")
registered_safety.start(sampling_config=SAMPLING)
print("  1/5 safety is active")

# ── 2. Professional tone (Guidelines) ─────────────────────────────────────────
professional_tone = Guidelines(
    name="professional_tone",
    guidelines=[
        "The response uses a professional and empathetic tone appropriate for healthcare communication.",
        "The response avoids jargon unless it is clearly explained.",
        "The response is respectful and sensitive to the member's situation.",
    ],
    model=JUDGE_MODEL,
)
registered_tone = professional_tone.register(name="professional_tone")
registered_tone.start(sampling_config=SAMPLING)
print("  2/5 professional_tone is active")

# ── 3. Response completeness (Guidelines) ─────────────────────────────────────
response_completeness = Guidelines(
    name="response_completeness",
    guidelines=[
        "The response fully addresses the member's question or request.",
        "The response includes actionable next steps or specific information the member can use.",
        "The response references relevant data (e.g., claim IDs, dates, appeal statuses) when available.",
    ],
    model=JUDGE_MODEL,
)
registered_completeness = response_completeness.register(name="response_completeness")
registered_completeness.start(sampling_config=SAMPLING)
print("  3/5 response_completeness is active")

# ── 4. Routing quality (make_judge, 3-level) ─────────────────────────────────
routing_quality = make_judge(
    name="routing_quality",
    instructions=(
        "You are evaluating whether a multi-agent supervisor correctly routed a "
        "member's question to the appropriate tool(s). The supervisor has access to "
        "tools for claims lookup, appeals status, member info, benefits, and general "
        "Medicare knowledge.\n\n"
        "Request: {{ inputs }}\n"
        "Response: {{ outputs }}\n\n"
        "Evaluate the routing quality."
    ),
    feedback_value_type=Literal["correct_routing", "partial_routing", "incorrect_routing"],
    model=JUDGE_MODEL,
)
registered_routing = routing_quality.register(name="routing_quality")
registered_routing.start(sampling_config=SAMPLING)
print("  4/5 routing_quality is active")

# ── 5. Appeals domain accuracy (make_judge, 3-level) ─────────────────────────
appeals_domain_accuracy = make_judge(
    name="appeals_domain_accuracy",
    instructions=(
        "You are a Medicare appeals domain expert. Evaluate whether the response "
        "contains accurate information about Medicare appeals processes, timelines, "
        "member rights, and regulatory requirements.\n\n"
        "Request: {{ inputs }}\n"
        "Response: {{ outputs }}\n\n"
        "Evaluate Medicare appeals domain accuracy."
    ),
    feedback_value_type=Literal["accurate", "mostly_accurate", "inaccurate"],
    model=JUDGE_MODEL,
)
registered_accuracy = appeals_domain_accuracy.register(name="appeals_domain_accuracy")
registered_accuracy.start(sampling_config=SAMPLING)
print("  5/5 appeals_domain_accuracy is active")

print("\nAll 5 scorers registered and started successfully.")
