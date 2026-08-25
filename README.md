# A RAG-Enhanced LLM Framework for Conversational Analytics

This project provides an AI-powered conversational analytics platform that allows business users to access enterprise spend and sales insights using natural language. By combining Large Language Models (LLMs), LangChain/LangGraph, Microsoft Fabric, and Power BI, the system eliminates the need for users to write SQL or navigate complex BI dashboards.

## The Problem

* **Scattered Data:** Business data is spread across dashboards, spreadsheets, and databases, making it difficult for non-technical users to quickly access insights.


* **Costly Dependency:** Decision-makers rely heavily on data analysts for basic answers, which slows down decision-making and consumes valuable analyst time on repetitive requests.


* **The SQL Barrier:** Traditional database querying requires SQL knowledge, creating a significant barrier for everyday business users.


* **Text-to-SQL Limitations:** Existing Text-to-SQL systems often struggle with complex schemas, ambiguous questions, and table relationships.


* **Safety and Security:** LLMs can generate unsafe or incorrect queries if they lack schema understanding and validation. Furthermore, enterprise environments mandate strict security, access control, and governance.



## How It Works

1. **Ask:** A user asks a question in plain natural language.


2. **Understand:** The LLM and RAG framework processes the intent, schema, and context.


3. **Retrieve:** Business data is securely retrieved via Microsoft Fabric through a secure query layer.


4. **Deliver:** The insight is delivered back to the user as a Power BI report or chart.



## System Architecture

The architecture is divided into four primary, sequential zones:

* **1. Microsoft Teams (User Interface):** The employee asks a question in plain English.


* **2. AI Layer (Azure):** This zone understands the intent, writes the SQL, and verifies that it is safe. It includes the LangGraph Workflow Manager, RAG/Schema Retrieval, LLM Text-to-SQL generation, SQL Validation, and Role-Based Access Control (RBAC).


* **3. Microsoft Fabric (Data Storage):** This serves as the Lakehouse/Warehouse, holding the company's real spend and sales data.


* **4. Response Delivery:** The final output is delivered via Teams Chat or Power BI as a chart summary or full interactive report.



## Technologies Used

* **LangChain:** A framework that organizes the LLM request into structured, reusable components instead of sending one large prompt string.


* **LangGraph:** Orchestrates every stage of the pipeline (intent classification, schema retrieval, SQL generation, guardrail checking, and execution) as explicit nodes in a graph. It also turns manual retry logic into a self-healing loop that routes failed executions back to the guardrail step.


* **Retrieval-Augmented Generation (RAG):** Bridges the LLM's general knowledge with private enterprise data, allowing the AI to dynamically search, read, and cite internal documents before generating an answer.



## References

* Wang et al., RAT-SQL, ACL 2020.


* Scholak et al., PICARD, EMNLP 2021.


* Wang et al., LinkAlign, EMNLP 2025.


* Yu et al., CoSQL, EMNLP-IJCNLP 2019.


* Song et al., SecureSQL, Findings of EMNLP 2024.


* Fei et al., Benchmarking Text-to-SQL under Role-Based Access Control, 2026.
