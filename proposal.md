# Schema Drift Detection and Auto-Resolution Agent Proposal

### **Objective**  

The goal of this 6-week project is to develop a **Schema Drift Detection and Auto-Resolution Agent** that can automatically identify changes in data schemas and suggest code updates to accommodate those changes. Schema drift refers to unexpected changes in the structure of data (e.g. adding, removing, or modifying columns, data types, etc.) that often break ETL pipelines. This project aims to build a prototype agent that detects schema drift in real-time and uses a Large Language Model (LLM) to generate suggestions to update downstream data pipelines accordingly. By automating schema comparison and resolution, the agent will reduce firefighting efforts and keep data pipelines running smoothly with minimal human intervention.

### **Business Relevance**

Schema drift is a persistent challenge in data engineering that can disrupt analytics and require urgent fixes. For The Modern Data Company, an automated solution adds significant value by:

- **Reducing Downtime:** Automatically detecting and addressing schema changes helps prevent pipeline failures, ensuring continuous data flow for analytics and reporting. This accelerates remediation and reduces errors, so data teams can focus on insights rather than fixing broken pipelines.

- **Improving Agility:** The agent enables the organization to adapt quickly when upstream systems change. This is crucial for maintaining DataOS platform reliability, as data sources and formats evolve.

- **Cutting Maintenance Cost:** By minimizing manual intervention, the solution lowers the maintenance burden on engineers. It handles routine schema tweaks, allowing engineers to devote time to higher-value tasks.

- **Innovating with AI:** Incorporating an LLM-driven agent showcases The Modern Data Company’s commitment to innovation. Industry leaders are already leveraging AI to handle schema drift. Adopting a similar approach in our workflows keeps us competitive and forward-looking.

### **Technical Scope**

This project will involve building several key components to detect schema drift and suggest resolutions:

- **Schema Comparison Engine:** Develop a module to **compare schemas** of data tables or streams over time. This could involve reading table schemas from Databricks and detecting differences such as new columns, removed columns, or changed data types. The output will be a structured **diff** highlighting the drift (for example, “Column X (STRING) added” or “Column Y type changed from INT to FLOAT”).

- **LLM Prompt Generation:** Implement logic to create informative prompts for a Large Language Model Given the detected schema differences, the prompt will include context such as:
  - Description of the source schema vs. target schema (or previous vs. current schema).
  - The specific changes detected (e.g. “*a new column customer_age (INT) was added to the source*”).
  - The desired outcome (e.g. “*update the transformation code to include the new column with appropriate logic*” or “*generate an ALTER TABLE statement to add the new column in the target table*”).
  - Any constraints or style guidelines (for example, “*provide PySpark code snippet*”).

- **Auto-Resolution Suggestion:** The agent will feed the prompt to the LLM and obtain code suggestions. These suggestions may include:
  - PySpark code to adjust an ETL job (e.g. adding a .withColumn transformation for a new field or handling a renamed column).
  - SQL commands to modify table schema (e.g. an ALTER TABLE to add/drop).
  - Markdown or pseudo-code explanations if needed, though the focus is on executable code.
  
  The agent will then validate these suggestions (at least superficially) – for example, checking that the suggested code compiles or the SQL syntax is correct for Spark. Final decisions to apply changes can be left to engineers after review, ensuring a human-in-the-loop for production safety.

- **Integration Hooks:** The prototype will run as a Databricks notebook or job that periodically checks for schema updates for now. Alternatively, it can be triggered by events (for instance, a new file arrival in a data lake triggering schema inference). While full automation of dependency tracking is advanced, the project will focus on a manageable scope (e.g. one pipeline or a set of linked tables) and lay the groundwork for broader lineage integration.

### **Tools and Technologies**

For this phase of the project, all development and testing will be done within the Databricks Platform:

- **Databricks Notebook:** Primary development environment. Will be used to develop the schema detection, LLM prompt formatting, and code suggestions. Databricks provides a collaborative workspace, and the compute needed for data processing with PySpark.

- **Databricks Utilities:** For accessing schema metadata and displaying outputs cleanly within notebooks.

- **PySpark:** Used to read table schemas, process data, and implement any ETL logic. PySpark’s DataFrame API will help in programmatically comparing schemas and applying any suggested transformations.

- **Delta Lake:** As the data storage format, Delta Lake tables will be the subject of schema drift detection. Delta Lake supports schema enforcement and evolution (for example, adding new columns automatically when enabled).

### **Learning Goals**

- **Prompt Engineering for LLMs:** Gain hands-on experience in crafting prompts to get useful outputs from a large language model. This involves iterating on how to clearly describe a problem (schema differences) to an AI and refining prompts when outputs are inaccurate.

- **Delta Lake Architecture & Data Pipeline Design:** Deepen understanding of the Delta Lake based data architecture. The project offers exposure to how data schemas evolve in a pipeline and how Delta Lake manages schema enforcement and evolution.

- **ETL Automation & PySpark Best Practices:** By building an automated agent, I will improve my PySpark and ETL development skills. I will learn how to programmatically inspect schemas (DataFrame.schema), compare structures, and generate transformations on the fly. This also involves writing robust PySpark code that can handle changes gracefully (for example, providing default values for new columns or mapping old column names to new ones). The project emphasizes automating what is usually a manual ETL maintenance task, thereby strengthening his ability to write flexible, resilient data pipelines.

- **Integration of AI in Data Engineering:** This project sits at the intersection of AI and data engineering. I will gain practical experience in calling AI services from within a data pipeline and handling the responses. I’ll confront real-world issues such as interpreting the AI’s output, validating it, and potentially handling errors or hallucinations from the LLM. This is a cutting-edge skill as enterprises begin to blend AI into their data toolsets.

- **Project Autonomy and Experimentation:** The internship project encourages a high degree of autonomy and experimentation. I will practice taking a problem from concept to solution – including researching approaches (e.g. reading how others tackle schema drift), making design decisions, and iteratively improving the solution. This will improve my ability to structure a project and adapt when things don’t go as expected (for example, if initial LLM outputs are not usable, he’ll experiment with different prompt strategies or fallback solutions).

### **Milestones and Timeline (6 Weeks)**

To ensure steady progress, the project is divided into weekly milestones with clear goals:

- **Week 1: Research & Planning**
  - **Task:** Conduct thorough research on schema drift scenarios and existing solutions. Study how Databricks and Delta Lake handle schema changes. Review related tools and concepts (e.g., schema evolution, merge schema behavior).
  - **Output:** A written design plan defining schema change types to handle (e.g., column additions, deletions, type changes), high-level approach, and initial prompt templates. Identify sample datasets and Delta tables for prototyping.

- **Week 2: Schema Drift Detection Module**
  - **Task:** Develop a PySpark-based schema comparison engine. Capture schemas from Delta tables and compare versions (e.g., old vs. new) to identify changes.
  - **Output:** A function or notebook that returns structured schema diffs (e.g., JSON showing added, removed, or changed columns). Validate accuracy with unit tests on test schemas.

- **Week 3: Prompt Design + LLM Setup**
  - **Task:** Implement logic to convert schema diffs into effective LLM prompts. Test LLM responses in a Databricks notebook.
  - **Output:** A prompt generation function and initial prompt template library. Integration with LLM, and notebook functions to return LLM-generated suggestions with basic error handling.

- **Week 4: LLM Integration & Code Suggestions**
  - **Task:** Integrate schema comparison and prompt system into an end-to-end flow within Databricks. Test complex schema drift cases.
  - **Output:** A notebook prototype that handles multi-column changes, data type changes, and column renaming. Log LLM suggestions for each case and allow manual review.

- **Week 5: Testing, Validation, and Optimization**
  - **Task:** Expand test cases and refine prompts for consistency. Implement safeguards against faulty or verbose LLM outputs.
  - **Output:** Full test suite of schema drift cases. Updated prompt logic with formatting rules, fallback handling, and improved suggestions.

- **Week 6: Documentation & Final Demo**
  - **Task:** Finalize documentation and try to prepare a demo. Summarize approach and design.
  - **Output:** Usage guide, architecture overview, presentation slides, and live demo or video walkthrough showing schema drift resolution end-to-end.

### **Expected Deliverables**

By the completion of the project, the following deliverables are expected:

- **Functional Prototype (Databricks Notebook):** A well-documented Databricks notebook (or series of notebooks) containing the code for the schema drift agent. It should be able to run through a demonstration scenario end-to-end. This notebook will include the schema detection logic, prompt generation, and the API call to the LLM, as well as example outputs.

- **Prompt Template Library:** A collection of curated prompt templates and examples used for the LLM. This could be delivered as a markdown document or within the code comments. It will describe how to prompt the LLM for different schema drift cases. (For example, a template for adding columns, one for removing columns, etc., possibly with a few shot examples of input-output.) This library will be useful for future improvements or if the prompts need to be fine-tuned further.

- **Test Cases and Data:** Any test scripts or small datasets used to validate the agent. For instance, a set of table schema files (in JSON or spark schema format) before and after drift, and the expected outcome. Likewise, if any pytest or assertion-based tests were written for components, those will be included in the repository.

- **Documentation:** Comprehensive documentation covering:
  - **User Guide:** How to use the prototype, how to configure it (e.g. setting API keys, targeting specific tables or data sources), and how to interpret its outputs. This guide should allow someone else to run the agent on their own schema changes.
  - **Technical Design:** An overview of the system design, explaining each component (detection, prompt, suggestion) and how they interact. It should also mention any decisions made (and why), and how we addressed challenges encountered (for example, how we dealt with poor LLM suggestions).