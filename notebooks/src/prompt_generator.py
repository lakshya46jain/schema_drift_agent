import sys
sys.path.append("/Workspace/Users/lakshya.jain@tmdc.io/schema_drift_agent/notebooks/src")

from custom_prompt_template import PromptTemplate

TEMPLATES = {
    "column_added_flat": PromptTemplate(
        name = "column_added_flat",
        metadata = {
            "max_tokens": 100,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a Data Engineering Assistant embedded in Databricks, specialized in incremental schema updates."
            ),
            "DOMAIN_CONTEXT": (
                "Working with Delta Lake tables in PySpark."
                "Schema changes include adding new columns." 
                "Pipeline must handle added fields gracefully."
            ),
            "CAPABILITIES_AND_TOOLS": (
                "PySpark DataFrame APIs (withColumn, F.lit, .cast)"
                "Access to field metadata or JSON diff as context"
                "Markdown code block generation"
            ),
            "TASK_DEFINITION":(
                "Given a new column, produce a PySpark snippet that adds it to DataFrame df with a default null cast to the appropriate type."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Use df.withColumn('{col}', F.lit(None).cast('{to_type}'))"
                "Use exact column name and type"
                "Output only the code block (no explanation)"
                "Do not drop or rename any columns"
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final code."
            ),
            "OUTPUT_CONTRACT": (
                "```python\n"
                "df = df.withColumn(\"{col}\", F.lit(None).cast(\"{to_type}\"))\n"
                "```\n"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY":(
                "If column type is missing or unrecognized, assume StringType. Prompt fallback logic should handle defaults."
            ),
            "SAFETY_AND_GUARDRAILS":(
                "Never reference columns outside added list. Do not include comments or logs."
            )
        }
    ),
    "column_removed_flat": PromptTemplate(
        name = "column_removed_flat",
        metadata = {
            "max_tokens": 100,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a Data Engineering Assistant embedded in Databricks, tasked with cleaning up ETL logic after schema drift."
            ),
            "DOMAIN_CONTEXT": (
                "A column has been removed from the source schema."
                "ETL pipelines will fail if they attempt to reference it in transformations."
            ),
            "CAPABILITIES_AND_TOOLS": (
                "PySpark DataFrame API: drop('{col}')"
            ),
            "TASK_DEFINITION":(
                "Generate PySpark code to drop the removed column from DataFrame df."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Use df.drop('{col}')"
                "Do not modify or drop other columns"
                "Output only the code block (no explanation)"
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final code."
            ),
            "OUTPUT_CONTRACT": (
                "```python\n"
                "df = df.drop(\"{col}\")\n"
                "```\n"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY":(
                "If the column isn’t present in the DataFrame, return df unchanged."
            ),
            "SAFETY_AND_GUARDRAILS":(
                "Ensure only the specified column is dropped."
            )
        }
    ),
    "type_changed_flat": PromptTemplate(
        name = "type_changed_flat",
        metadata = {
            "max_tokens": 100,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a PySpark code assistant responsible for adapting DataFrames to reflect schema type changes."
            ),
            "DOMAIN_CONTEXT": (
                "The column {col} has changed from {from_type} to {to_type}."
                "The update should be applied to df, preserving all other columns." 
            ),
            "CAPABILITIES_AND_TOOLS": (
                "PySpark withColumn() and cast() APIs"
                "Access to schema diff metadata"
            ),
            "TASK_DEFINITION":(
                "Cast the column to its new type using PySpark and return the modified DataFrame."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Output only the code block (no explanation)"
                "Use F.col(\"{col}\").cast(\"{to_type}\") inside withColumn"
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final code."
            ),
            "OUTPUT_CONTRACT": (
                "```python\n"
                "df = df.withColumn(\"{col}\", F.col(\"{col}\").cast(\"{to_type}\"))\n"
                "```\n"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY":(
                "If target type is unknown, assume StringType."
            ),
            "SAFETY_AND_GUARDRAILS":(
                "Only modify the specified column."
            )
        }
    ),
    "rename_flat": PromptTemplate(
        name = "rename_flat",
        metadata = {
            "max_tokens": 100,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a PySpark assistant updating column names based on schema drift."
            ),
            "DOMAIN_CONTEXT": (
                "The field {from_col} was renamed to {to_col}."
                "No other structural or type change occurred." 
            ),
            "CAPABILITIES_AND_TOOLS": (
                "PySpark .withColumnRenamed() API"
            ),
            "TASK_DEFINITION":(
                "Generate PySpark code to rename column {from_col} to {to_col} in DataFrame df."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Do not cast or drop columns"
                "Output only the code block (no explanation)"
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final code."
            ),
            "OUTPUT_CONTRACT": (
                "```python\n"
                "df = df.withColumnRenamed(\"{from_col}\", \"{to_col}\")\n"
                "```\n"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY":(
                "If either field is missing, skip transformation."
            ),
            "SAFETY_AND_GUARDRAILS":(
                "Only rename if both field names are valid."
            )
        }
    ),
}
    
DRIFT_TEMPLATE_MAP = {
    "added": {
        "flat": "column_added_flat"
    },
    "removed": {
        "flat": "column_removed_flat"
    },
    "type_changed": {
        "flat": "type_changed_flat"
    },
    "renamed": {
        "flat": "rename_flat"
    }
}

def generate_prompts(diff: dict) -> list:
    prompts = []

    # Handle added columns (now items are dicts)
    for item in diff.get("added", []):
        tpl = TEMPLATES[DRIFT_TEMPLATE_MAP["added"]["flat"]]
        values = {
            "col": item["col"],
            "to_type": item.get("to_type")
        }
        prompts.append({
            "template": tpl.name,
            "prompt": tpl.render(values)
        })

    # Handle removed columns
    for col in diff.get("removed", []):
        tpl = TEMPLATES[DRIFT_TEMPLATE_MAP["removed"]["flat"]]
        values = {"col": col}
        prompts.append({
            "template": tpl.name,
            "prompt": tpl.render(values)
        })

    # Handle type_changed entries (each is a dict)
    for change in diff.get("type_changed", []):
        tpl = TEMPLATES[DRIFT_TEMPLATE_MAP["type_changed"]["flat"]]
        values = {
            "col": change["col"],
            "from_type": change.get("from_type"),
            "to_type": change.get("to_type")
        }
        prompts.append({
            "template": tpl.name,
            "prompt": tpl.render(values)
        })

    # Handle renamed fields (each is a dict)
    for ren in diff.get("renamed", []):
        tpl = TEMPLATES[DRIFT_TEMPLATE_MAP["renamed"]["flat"]]
        values = {
            "from_col": ren["from_col"],
            "to_col": ren["to_col"]
        }
        prompts.append({
            "template": tpl.name,
            "prompt": tpl.render(values)
        })

    return prompts