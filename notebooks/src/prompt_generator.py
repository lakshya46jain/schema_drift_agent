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
            "TASK_DEFINITION": (
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
                "```"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If column type is missing or unrecognized, assume StringType. Prompt fallback logic should handle defaults."
            ),
            "SAFETY_AND_GUARDRAILS": (
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
            "TASK_DEFINITION": (
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
                "```"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If the column isn’t present in the DataFrame, return df unchanged."
            ),
            "SAFETY_AND_GUARDRAILS": (
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
            "TASK_DEFINITION": (
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
                "```"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If target type is unknown, assume StringType."
            ),
            "SAFETY_AND_GUARDRAILS": (
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
            "TASK_DEFINITION": (
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
                "```"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If either field is missing, skip transformation."
            ),
            "SAFETY_AND_GUARDRAILS": (
                "Only rename if both field names are valid."
            )
        }
    ),
    "combined_drift": PromptTemplate(
        name = "combined_drift",
        metadata = {
            "max_tokens": 200,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a Data Engineering Assistant embedded in Databricks, specialized in applying multiple schema drift resolutions in a single transformation sequence."
            ),
            "DOMAIN_CONTEXT": (
                "Working with Delta Lake tables in PySpark."
                "Schema changes may include added columns, removed columns, and type changes."
                "The DataFrame df must be updated in the correct sequence to avoid errors and maintain downstream compatibility."
            ),
            "CAPABILITIES_AND_TOOLS": (
                "PySpark DataFrame APIs: withColumn, drop, cast"
                "Access to field metadata or JSON diff as context"
                "Ability to chain multiple transformations on a single DataFrame"
                "Markdown code block generation"
            ),
            "TASK_DEFINITION": (
                "Given detected schema drift, generate a PySpark snippet that sequentially:"
                "1. Drops removed columns"
                "2. Adds new columns with default null values cast to the correct type"
                "3. Casts existing columns with changed types to their new types"
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Preserve exact transformation order: drop → add → cast"
                "Use df.drop('{old_col}') for removed columns"
                "Use df.withColumn('{col}', F.lit(None).cast('{to_type}')) for added columns"
                "Use df.withColumn('{col}', F.col('{col}').cast('{to_type}')) for type-changed columns"
                "Output only the code block (no explanation)"
                "Do not rename or reorder unrelated columns"
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final code."
            ),
            "OUTPUT_CONTRACT": (
                "```python\n"
                "df = df.drop(\"{old_col}\") \\\n"
                "       .withColumn(\"{new_col}\", F.lit(None).cast(\"{to_type}\")) \\\n"
                "       .withColumn(\"{col}\", F.col(\"{col}\").cast(\"{to_type}\"))\n"
                "```"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If a column in the diff is not present in df, skip that transformation."
            ),
            "SAFETY_AND_GUARDRAILS": (
                "Never perform drop, add, or cast on columns not listed in the diff."
            ),
        }
    ),
    "type_changed_few_shot": PromptTemplate(
        name = "type_changed_few_shot",
        metadata = {
            "max_tokens": 250,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a Data Engineering Assistant embedded in Databricks, specialized in adapting PySpark DataFrames to reflect schema type changes."
            ),
            "DOMAIN_CONTEXT": (
                "Working with Delta Lake tables in PySpark."
                "Schema changes include type modifications for existing columns."
                "Pipeline must cast affected columns to their new data types while preserving all other columns."
            ),
            "CAPABILITIES_AND_TOOLS": (
                "PySpark DataFrame APIs (withColumn, F.col, .cast)"
                "Access to field metadata or JSON diff as context"
                "Markdown code block generation"
            ),
            "TASK_DEFINITION": (
                "Given a column with a type change, produce a PySpark snippet that casts it from {from_type} to {to_type}."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Use df.withColumn('{col}', F.col('{col}').cast('{to_type}'))"
                "Use exact column name and type"
                "Output only the code block (no explanation)"
                "Do not drop, rename, or modify any other columns"
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final code."
            ),
            "OUTPUT_CONTRACT": (
                "```python\n"
                "df = df.withColumn(\"{col}\", F.col(\"{col}\").cast(\"{to_type}\"))\n"
                "```"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If target type is missing or unrecognized, assume StringType. Prompt fallback logic should handle defaults."
            ),
            "SAFETY_AND_GUARDRAILS": (
                "Never reference columns outside the type_changed list."
                "Do not include comments or logs."
                "Follow the style from provided examples when applicable."
            )
        }
    ),
    "column_added_flat_sql": PromptTemplate(
        name = "column_added_flat_sql",
        metadata = {
            "max_tokens": 80,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a Data Engineering Assistant embedded in Databricks, specialized in generating SQL DDL statements for schema resolution in Delta tables."
            ),
            "DOMAIN_CONTEXT": (
                "Working with Delta Lake tables in SQL."
                "Schema changes include adding new columns."
                "DDL changes must preserve table integrity."
            ),
            "CAPABILITIES_AND_TOOLS": (
                "Generate ALTER TABLE ... ADD COLUMNS statements."
                "Access to field metadata or JSON diff as context."
            ),
            "TASK_DEFINITION": (
                "Given a new column, produce a SQL statement that adds it to a Delta Lake table with the appropriate type."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Use: ALTER TABLE target_table ADD COLUMNS ({col} {to_type});"
                "Use exact column name and type."
                "Follow Delta Lake SQL syntax."
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final SQL statement."
            ),
            "OUTPUT_CONTRACT": (
                "ALTER TABLE target_table ADD COLUMNS ({col} {to_type});"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If column type is missing or unrecognized, assume STRING."
            ),
            "SAFETY_AND_GUARDRAILS": (
                "Never reference columns outside added list."
                "Output only SQL, with no comments or markdown."
            )
        }
    ),
    "column_removed_flat_sql": PromptTemplate(
        name = "column_removed_flat_sql",
        metadata = {
            "max_tokens": 80,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a Data Engineering Assistant embedded in Databricks, specialized in generating SQL DDL statements for schema drift recovery in Delta Lake tables."
            ),
            "DOMAIN_CONTEXT": (
                "Working with Delta Lake tables in SQL."
                "Schema changes include removing columns."
                "DDL changes must update the table definition accordingly."
            ),
            "CAPABILITIES_AND_TOOLS": (
                "Generate ALTER TABLE ... DROP COLUMN statements."
                "Access to field metadata or JSON diff as context."
            ),
            "TASK_DEFINITION": (
                "Given a removed column, produce a SQL statement that drops it from a Delta Lake table."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Use: ALTER TABLE target_table DROP COLUMN {col};"
                "Use exact column name."
                "Follow Delta Lake SQL syntax."
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final SQL statement."
            ),
            "OUTPUT_CONTRACT": (
                "ALTER TABLE target_table DROP COLUMN {col};"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If multiple columns are removed, generate separate statements for each."
            ),
            "SAFETY_AND_GUARDRAILS": (
                "Never reference columns outside removed list."
                "Output only SQL, with no comments or markdown."
            )
        }
    ),
    "type_changed_flat_sql": PromptTemplate(
        name = "type_changed_flat_sql",
        metadata = {
            "max_tokens": 80,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a Data Engineering Assistant embedded in Databricks, specialized in generating SQL DDL statements to resolve schema type changes in Delta Lake tables."
            ),
            "DOMAIN_CONTEXT": (
                "Working with Delta Lake tables in SQL."
                "Schema changes include column type modifications."
                "DDL changes must alter the column definition to match the new type."
            ),
            "CAPABILITIES_AND_TOOLS": (
                "Generate ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE statements."
                "Access to field metadata or JSON diff as context."
            ),
            "TASK_DEFINITION": (
                "Given a column with a changed type, produce a SQL statement to alter its data type in a Delta Lake table."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Use: ALTER TABLE target_table ALTER COLUMN {col} SET DATA TYPE {to_type};"
                "Use exact column name and correct SQL type."
                "Follow Delta Lake SQL syntax."
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final SQL statement."
            ),
            "OUTPUT_CONTRACT": (
                "ALTER TABLE target_table ALTER COLUMN {col} SET DATA TYPE {to_type};"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If the new type is missing or unrecognized, fallback to STRING."
            ),
            "SAFETY_AND_GUARDRAILS": (
                "Only alter the specified column."
                "Output only SQL, with no comments or markdown."
            )
        }
    ),
    "rescued_data_streaming": PromptTemplate(
        name = "rescued_data_streaming",
        metadata = {
            "max_tokens": 100,
            "llm": "openai/gpt-4o"
        },
        sections = {
            "ROLE": (
                "You are a Data Engineering Assistant embedded in Databricks, specialized in recovering fields from the _rescued_data column during streaming schema drift handling."
            ),
            "DOMAIN_CONTEXT": (
                "Auto Loader is configured in rescue mode."
                "Unexpected or untyped fields are automatically captured in the _rescued_data column."
                "The engineer needs to extract specific fields for validation or inclusion in downstream pipelines."
            ),
            "CAPABILITIES_AND_TOOLS": (
                "Access fields inside _rescued_data using get_json_object() or from_json() depending on format."
                "Generate PySpark DataFrame API code to extract and cast these fields."
            ),
            "TASK_DEFINITION": (
                "Given a field name and target type, produce PySpark code to extract the field from _rescued_data and cast it to the correct type."
            ),
            "CONSTRAINTS_AND_POLICIES": (
                "Use: df.withColumn(\"{col}\", F.get_json_object(F.col(\"_rescued_data\"), \"$.{col}\").cast(\"{to_type}\"))"
                "Do not assume schema beyond the given field."
                "Assign extracted data to a new column with the same name as the field."
            ),
            "REASONING_DIRECTIVE": (
                "Think step-by-step silently. Then return only the final code."
            ),
            "OUTPUT_CONTRACT": (
                "```python\n"
                "df = df.withColumn(\"{col}\", F.get_json_object(F.col(\"_rescued_data\"), \"$.{col}\").cast(\"{to_type}\"))\n"
                "```"
            ),
            "AMBIGUITY_OR_RECOVERY_STRATEGY": (
                "If the field is not present, the resulting column should contain nulls. No failure or exception handling logic required."
            ),
            "SAFETY_AND_GUARDRAILS": (
                "Only access fields inside _rescued_data using the JSON path syntax ($.field_name)."
                "Do not alter other parts of the DataFrame."
            )
        }
    )
}
    
DRIFT_TEMPLATE_MAP = {
    "added": {
        "flat": "column_added_flat",
        "flat_sql": "column_added_flat_sql"
    },
    "removed": {
        "flat": "column_removed_flat",
        "flat_sql": "column_removed_flat_sql"
    },
    "type_changed": {
        "flat": "type_changed_flat",
        "flat_sql": "type_changed_flat_sql"
    },
    "renamed": {
        "flat": "rename_flat"
    },
    "rescued_data": {
        "streaming": "rescued_data_streaming"
    }
}


def generate_prompts(diff: dict, backends=("flat",)) -> list:
    """
    Render prompts for one or more backends based on DRIFT_TEMPLATE_MAP.
    backends can include: "flat" (PySpark), "flat_sql" (DDL), "streaming" (rescued data).
    """
    prompts = []

    # --- Helpers -------------------------------------------------------------
    def to_sql_type(t):
        """Best-effort Spark -> SQL type mapping (fallback to original)."""
        mapping = {
            "ByteType": "TINYINT",
            "ShortType": "SMALLINT",
            "IntegerType": "INT",
            "LongType": "BIGINT",
            "FloatType": "FLOAT",
            "DoubleType": "DOUBLE",
            "DecimalType": "DECIMAL",
            "BooleanType": "BOOLEAN",
            "StringType": "STRING",
            "TimestampType": "TIMESTAMP",
            "DateType": "DATE",
            # Add more as needed; for complex types, leave as-is.
        }
        return mapping.get(t, t or "STRING")

    def render(kind: str, values: dict):
        mapping = DRIFT_TEMPLATE_MAP.get(kind, {})
        for backend in backends:
            key = mapping.get(backend)
            if not key:
                continue
            tpl = TEMPLATES.get(key)
            if not tpl:
                continue

            # Adjust values per backend without mutating the caller's dict
            _vals = dict(values)

            # SQL templates expect SQL-friendly types
            if backend.endswith("_sql") and "to_type" in _vals and isinstance(_vals["to_type"], str):
                _vals["to_type"] = to_sql_type(_vals["to_type"])

            prompts.append({
                "template": tpl.name,
                "prompt": tpl.render(_vals)
            })

    # --- Added columns -------------------------------------------------------
    for item in diff.get("added", []):  # item: {"col": ..., "to_type": ...}
        render("added", {
            "col": item.get("col"),
            "to_type": item.get("to_type") or "StringType"
        })

    # --- Removed columns -----------------------------------------------------
    for col in diff.get("removed", []):  # col: "name" or "nested.path"
        render("removed", {"col": col})

    # --- Type changes --------------------------------------------------------
    for change in diff.get("type_changed", []):  # {"col", "from_type", "to_type"}
        render("type_changed", {
            "col": change.get("col"),
            "from_type": change.get("from_type"),
            "to_type": change.get("to_type") or "StringType"
        })

    # --- Renames -------------------------------------------------------------
    for ren in diff.get("renamed", []):  # {"from_col", "to_col"}
        render("renamed", {
            "from_col": ren.get("from_col"),
            "to_col": ren.get("to_col")
        })

    # --- Rescued data (optional) --------------------------------------------
    for item in diff.get("rescued_data", []):
        render("rescued_data", {
            "col": item.get("col"),
            "to_type": item.get("to_type") or "StringType"
        })

    return prompts