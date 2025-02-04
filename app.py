from flask import Flask, request, jsonify
from flask_cors import CORS
from sqlalchemy import create_engine
import pandas as pd
import traceback
import openai
import os

app = Flask(__name__)
CORS(app, resources={r"/query": {"origins": "*"}}, supports_credentials=True)
# @app.route("/query", methods=["POST"])
# def query():
#     data = request.json
#     return jsonify({"message": "Received", "data": data})

def connectDB():
    db_config = {
        "host": "aws-0-ap-southeast-1.pooler.supabase.com",
        "port": "6543",
        "dbname": "postgres",
        "user": "postgres.egkmupciopviyycuuuym",
        "password": "O2fETdNwU9r4nmzy"
    }
    connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    engine = create_engine(connection_string)
    return engine


def generate_sql_query(prompt, db_schema):
    schema_description = "\n".join(
        f"Table: {table}, Columns: {', '.join(columns)}"
        for table, columns in db_schema.items()
    )
    openai.api_key = os.getenv('OPENAI_API_KEY')

    sql_prompt = f"""
    Database Schema:
    {schema_description}

    User Query:
    "{prompt}"

    Generate a valid SQL query that satisfies the user query. Do not include explanations or extra text; provide only the SQL query.
    """

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a SQL query generation assistant."},
            {"role": "user", "content": sql_prompt}
        ],
        max_tokens=200,
        temperature=0.5
    )

    sql_query = response['choices'][0]['message']['content'].strip()
    if sql_query.startswith("```") and sql_query.endswith("```"):
        sql_query = sql_query[3:-3].strip()

    return sql_query


def handle_subquery_error(sql_query):
    """
    Adjust the SQL query to handle cases where a subquery returns multiple rows.
    Replace `=` with `IN` where applicable.
    """
    if "= (" in sql_query:  # Detects potential subqueries that could cause issues
        sql_query = sql_query.replace("= (", "IN (")
    return sql_query


@app.route('/query', methods=['POST'])
def process_query():
    try:
        data = request.json
        query = data.get("query")
        if not query:
            return jsonify({"error": "Query is required"}), 400

        # Mock schema for testing purposes
        db_schema = {'avm_reminders_duplicate': ['id', 'company_id', 'user_id', 'frequency', 'last_reminder_sent_at', 'created_at', 'data', 'status', 'profile_id', 'next_reminder_time'], 'stocks_profile': ['id', 'created_at', 'User Name', 'Phone Number', 'Company', 'Stocks', 'Data'], 'profiles': ['id', 'created_at', 'name', 'type', 'email', 'phone', 'vc_company_name', 'data'], 'cs_requests': ['id', 'created_at', 'company_id', 'doc_id', 'doc_status', 'category', 'category_type', 'description', 'is_verified', 'add_stop', 'approver_id', 'add_stop_id', 'profile_id', 'stakeholder_ids', 'data', 'doc_ids', 'duration', 'doc_comments'], 'ir_templates': ['id', 'created_at', 'company_id', 'ir_template', 'name', 'profile_id', 'default_template', 'document_list'], 'market-insights': ['id', 'created_at', 'Industry', 'market-size-link', 'predictions', 'pdf_link', 'text_data', 'chart_data'], 'cs_categories': ['id', 'created_at', 'doc_id', 'company_id', 'profile_id', 'category', 'category_type', 'description', 'is_verified', 'data'], 'company_linkedin': ['id', 'linkedin', 'accessed', 'educations', 'headline', 'position', 'summary'], 'pitch_requests': ['id', 'doc_status', 'description', 'data', 'email_id', 'attachment_ids', 'approver_id', 'approver_ids', 'request_summary', 'verifier_approval_reason', 'request_details', 'profile_id', 'interview_questions', 'intro_doc_ids', 'intro_description', 'interview_transcribed_text', 'interview_grading', 'video_url', 'linkedin'], 'users': ['id', 'created_at', 'name', 'type', 'email', 'phone', 'data', 'vc_company_name', 'instance_id', 'id', 'aud', 'role', 'email', 'encrypted_password', 'email_confirmed_at', 'invited_at', 'confirmation_token', 'confirmation_sent_at', 'recovery_token', 'recovery_sent_at', 'email_change_token_new', 'email_change', 'email_change_sent_at', 'last_sign_in_at', 'raw_app_meta_data', 'raw_user_meta_data', 'is_super_admin', 'created_at', 'updated_at', 'phone', 'phone_confirmed_at', 'phone_change', 'phone_change_token', 'phone_change_sent_at', 'confirmed_at', 'email_change_token_current', 'email_change_confirm_status', 'banned_until', 'reauthentication_token', 'reauthentication_sent_at', 'is_sso_user', 'deleted_at', 'is_anonymous'], 'pitch_documents': ['id', 'created_at', 'name', 'company_id', 'user_id', 'doc_url', 'doc_type', 'is_processed', 'data', 'is_processing', 'profile_id', 'description', 'email_Id', 'is_signed', 'envelope_id', 'doc_summary'], 'public_data': ['id', 'created_at', 'founders_name', 'company_id', 'data', 'source_link'], 'network_members': ['id', 'created_at', 'name', 'email', 'designation', 'user_id', 'data', 'profile_id', 'network_member_profile_id'], 'gmail_tokens': ['id', 'token', 'refresh_token', 'token_uri', 'client_id', 'client_secret', 'expiry', 'account', 'universe_domain', 'scope', 'user_id'], 'cp_requests': ['id', 'created_at', 'company_id', 'doc_id', 'doc_status', 'category', 'category_type', 'description', 'is_verified', 'add_stop', 'approver_id', 'add_stop_id', 'profile_id', 'stakeholder_ids', 'data', 'doc_ids', 'doc_comments', 'duration'], 'ir_requests': ['id', 'company_id', 'ir_status', 'description', 'data', 'add_stop', 'add_stop_id', 'founder_id', 'email_id', 'profile_id', 'add_stop_approved', 'request_details', 'approver_ids', 'financial_year', 'submission_date', 'reporting_date', 'quarter', 'proof_ids', 'ir_ids'], 'vc_companies': ['id', 'name', 'type', 'email', 'website', 'linkedin', 'user_id', 'data', 'profile_id', 'preferred_partner'], 'founders_profile': ['id', 'created_at', 'companyName', 'founderName', 'resume', 'companyWebsite', 'foundersLinkedIn', 'companyType', 'preferredIndustries', 'otherIndustry', 'businessTypes', 'stageOfDevelopment', 'fundingStage', 'minMarket', 'marketShare', 'revenue', 'annualRunRate', 'countries', 'minAge', 'maxAge', 'minIncome', 'maxIncome', 'automationLevel', 'processesLevel', 'digitisationLevel', 'intellectualProperties', 'minFunding', 'monthlyExpenditure', 'minValuation', 'maxValuation', 'foundersBackground', 'keyHires', 'departments', 'revenueModel', 'customerAcquisition', 'states', 'cities', 'sex', 'languages', 'other', 'maxFunding', 'summary'], 'portfolio_companies': ['id', 'name', 'type', 'email', 'website', 'linkedin', 'user_id', 'data', 'profile_id', 'preferred_partner'], 'avm_requests_duplicate': ['id', 'company_id', 'avm_cat_id', 'consent_form_id', 'doc_status', 'description', 'add_stop', 'approver_id', 'add_stop_id', 'user_id', 'founder_id', 'data', 'email_id', 'profile_id', 'add_stop_approved'], 'doc_embeddings': ['id', 'content', 'embedding'], 'cp_reminders': ['id', 'company_id', 'user_id', 'frequency', 'last_reminder_sent_at', 'created_at', 'data', 'status', 'profile_id', 'next_reminder_time'], 'cs_reminders': ['id', 'created_at', 'company_id', 'data', 'profile_id', 'status', 'frequency', 'last_reminder_sent_at', 'next_reminder_time'], 'avm_categories': ['id', 'created_at', 'doc_id', 'company_id', 'user_id', 'category', 'category_type', 'description', 'is_verified', 'data', 'profile_id'], 'document_insights': ['id', 'doc_url', 'company_id', 'user_id', 'created_at', 'category', 'doc_type', 'description_url', 'is_verified', 'profile_id', 'document_id', 'data'], 'stakeholders': ['id', 'name', 'email', 'designation', 'company_id', 'user_id', 'profile_id', 'stakeholder_profile_id'], 'cp_categories': ['id', 'created_at', 'doc_id', 'company_id', 'profile_id', 'category', 'category_type', 'description', 'is_verified', 'data'], 'avm_reminders': ['id', 'company_id', 'user_id', 'frequency', 'last_reminder_sent_at', 'created_at', 'data', 'status', 'profile_id', 'next_reminder_time'], 'ir_reminders': ['id', 'company_id', 'user_id', 'frequency', 'last_reminder_sent_at', 'created_at', 'data', 'status', 'profile_id', 'next_reminder_time'], 'avm_requests': ['id', 'company_id', 'consent_form_id', 'doc_status', 'description', 'add_stop', 'user_id', 'founder_id', 'data', 'email_id', 'profile_id', 'add_stop_approved', 'avm_cat_ids', 'attachment_ids', 'add_stop_id', 'approver_id', 'approver_ids', 'attachments_url', 'request_detail', 'verifier_approval_reason'], 'processed_emails': ['id', 'email_id', 'processed_at'], 'vcs_profile': ['id', 'company_name', 'company_type', 'industry_type', 'industry_selection', 'additional_other_industry', 'business_type', 'other_constraints_category', 'other_constraints_description', 'development', 'development_selection', 'startup_funding', 'funding_selection', 'market_size', 'market_share', 'revenue', 'revenue_amount', 'category', 'constraints', 'countries', 'states', 'cities', 'initial_age', 'final_age', 'gender', 'min_income', 'max_income', 'language', 'other_info', 'level_of_automation', 'digitisation', 'intellectual_properties', 'other_constraints_tech', 'funding_capacity', 'monthly_expenditure', 'valuation', 'other_constraints_finance', 'processes_in_place', 'experience', 'college', 'customer', 'stakeholder', 'stakeholder_constraints', 'revenue_model', 'data'], 'startup-profile': ['company_id', 'created_at', 'Startup Name', 'IndustryType', 'Founder Name', 'Pre Seed Funding', 'Pre Seed Revenue', 'Seed Funding', 'Seed Revenue', 'Series A Revenue', 'Series A Funding', 'Series B Funding', 'Series B Revenue', 'Series C Funding', 'Series C Revenue', 'Annual Runrate', 'Article URL', 'Headlines'], 'documents': ['id', 'created_at', 'name', 'company_id', 'user_id', 'doc_url', 'doc_type', 'is_processed', 'data', 'is_processing', 'profile_id', 'description', 'email_Id', 'is_signed', 'envelope_id', 'details']}


        # Generate SQL query
        sql_query = generate_sql_query(query, db_schema)
        sql_query = handle_subquery_error(sql_query)  # Adjust for multi-row subqueries

        print(f"Query received: {query}")
        print(f"SQL Query Generated: {sql_query}")

        # Execute SQL query
        engine = connectDB()
        with engine.connect() as connection:
            try:
                results = pd.read_sql(sql_query, connection)

                if results.empty:
                    return jsonify({
                        "message": "No data found",
                        "raw_query": sql_query,
                        "explanation": explain_query(sql_query),
                        "suggested_queries": suggest_related_queries(query)
                    }), 404

                # Prepare response
                response = {
                    "results": results.to_dict(orient='records'),
                    "raw_query": sql_query,
                    "explanation": explain_query(sql_query),
                    "suggested_queries": suggest_related_queries(query)
                }
                return jsonify(response), 200
            except Exception as e:
                print(f"Error executing SQL query: {e}")
                return jsonify({"error": str(e)}), 500
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


def explain_query(sql_query):
    """
    Generate an explanation of what the given SQL query does using OpenAI.
    """
    explanation_prompt = f"""
    Explain the following SQL query in simple terms:
    {sql_query}
    """
    openai.api_key = os.getenv('OPENAI_API_KEY')

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an SQL query explanation assistant."},
            {"role": "user", "content": explanation_prompt}
        ],
        max_tokens=150,
        temperature=0.3
    )

    explanation = response['choices'][0]['message']['content'].strip()
    return explanation


def suggest_related_queries(user_query):
    """
    Generate three related queries based on the original user query using OpenAI.
    """
    suggestion_prompt = f"""
    The user provided this query: "{user_query}".
    Generate 3 related SQL queries that might also be useful to the user.
    Provide them as a simple list.
    """
    openai.api_key = os.getenv('OPENAI_API_KEY')

    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an SQL query suggestion assistant."},
            {"role": "user", "content": suggestion_prompt}
        ],
        max_tokens=200,
        temperature=0.5
    )

    suggestions = response['choices'][0]['message']['content'].strip().split("\n")
    suggestions = [s.strip("- ") for s in suggestions if s.strip()]
    return suggestions[:3]  # Limit to 3 suggestions

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

