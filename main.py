
class UserDataExtractor:
    def __init__(self, text=""):
        # Initialize with optional text
        self.text = text

    def extract_user_information_subtext(self, text):
        """Extract the '# USER INFORMATION' section from the given text."""
        start_index = text.find("# USER INFORMATION")
        if start_index != -1:
            return text[start_index:]
        else:
            return ""
        
    def extract_children_of_household(self, text):
        """Extract the '## Children of the household' section from the given text."""
        # Find the start of the children section
        start_index = text.find("## Children of the household")
        
        if start_index == -1:
            return ""
        
        # Find the end of the children section, which ends before the next section starts
        # The next section starts with "##" which marks the beginning of a new section
        end_index = text.find("\n\n", start_index + 1)
        
        if end_index == -1:
            # If there's no further section, it means we're at the end of the text
            end_index = len(text)
            
        children_section = text[start_index+30:end_index].strip()
        
        return children_section
    
    def extract_adult_of_household(self, text):
        """Extract the '## Adults of the household' section from the given text."""
        # Find the start of the children section
        start_index = text.find("## Adults of the household")
        
        if start_index == -1:
            return ""
        
        # Find the end of the children section, which ends before the next section starts
        # The next section starts with "##" which marks the beginning of a new section
        end_index = text.find("\n\n", start_index + 1)
        
        if end_index == -1:
            # If there's no further section, it means we're at the end of the text
            end_index = len(text)
            
        adult_section = text[start_index+28:end_index].strip()
        
        return adult_section
    
    def extract_orientation(self, text):
        """Extract the '## Orientation' section from the given text."""
        # Find the start of the children section
        start_index = text.find("# O-rientation Completed")
        
        if start_index == -1:
            
            start_index = text.find("## Orientation Progress")
            
            if start_index == -1:
                return ""
                
            orient_section = text[start_index+24:].strip()
            
            return orient_section
    
        else:
            return "The user has completed their O-rientation. If the user asks about O-rientation, you can inform them that they have completed it."
        
    
    def extract_specific_user_details(self, text):
        """Extract specific user details such as name, email, and calendar status."""
        # Default values for extracted details
        name = ""
        last_name = ""
        email = ""
        personalized_email = ""
        household_member_id = ""
        calendar_connected = ""

        # Extract Name
        name_match = re.search(r"- Name: ([^\n]+)", text)
        if name_match:
            name = name_match.group(1)

        # Extract Last Name
        last_name_match = re.search(r"- Last name: ([^\n]+)", text)
        if last_name_match:
            last_name = last_name_match.group(1)

        # Extract Email
        email_match = re.search(r"- Email: ([^\n]+)", text)
        if email_match:
            email = email_match.group(1)

        # Extract Personal Assistant's Email
        assistant_email_match = re.search(r"- Personal Assistant's Email: ([^\n]+)", text)
        if assistant_email_match:
            personalized_email = assistant_email_match.group(1)

        # Extract Household Member ID
        household_id_match = re.search(r"- Household Member ID: ([^\n]+)", text)
        if household_id_match:
            household_member_id = household_id_match.group(1)

        # Extract Calendar Connected Status
        calendar_connected_match = re.search(r"- Calendar is connected", text)
        if calendar_connected_match:
            calendar_connected = "Yes"

        return {
            "name": name,
            "last_name": last_name,
            "email": email,
            "personalized_email": personalized_email,
            "household_member_id": household_member_id,
            "calendar_connected": calendar_connected
        }

    def extract_memo_pad(self, text):
        """Extract the content under the '## Memo Pad' section."""
        memo_pad_pattern = r'## Memo Pad:(.*?)(?=\n##|\Z)'
        match = re.search(memo_pad_pattern, text, re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""

    def enrich_dataframe_with_extracted_info(self, df_gh):
        """Enrich the DataFrame with extracted user details and memo pads."""
        extracted_data = {
            'Name': [],
            'Last Name': [],
            'Email': [],
            'Personal Assistant Email': [],
            #'Household Member ID': [],
            'Calendar Connected': [],
            'Memo Pad': [],
            'user_profile_text_summary': [],
            'Children of the Household': [],
            "Adults of the Household": [],
            "orientation_progess": []
        }

        kane = df_gh['system_prompt'].tolist()
        for text in kane:
            text = str(text)
            user_details = self.extract_specific_user_details(text)
            memo_pad_content = self.extract_memo_pad(text)
            user_profile_text_summary = self.extract_user_information_subtext(text)
            child = self.extract_children_of_household(text)
            adult = self.extract_adult_of_household(text)
            orient = self.extract_orientation(text)

            # Append extracted details to corresponding lists
            extracted_data['Name'].append(user_details["name"])
            extracted_data['Last Name'].append(user_details["last_name"])
            extracted_data['Email'].append(user_details["email"])
            extracted_data['Personal Assistant Email'].append(user_details["personalized_email"])
            #extracted_data['Household Member ID'].append(user_details["household_member_id"])
            extracted_data['Calendar Connected'].append(user_details["calendar_connected"])
            extracted_data['Memo Pad'].append(memo_pad_content)
            extracted_data['user_profile_text_summary'].append(user_profile_text_summary)
            extracted_data['Children of the Household'].append(child)
            extracted_data['Adults of the Household'].append(adult)
            extracted_data['orientation_progess'].append(orient)

        # Add extracted data to the DataFrame as new columns
        for key, value in extracted_data.items():
            df_gh[key] = value

        return df_gh
    
    
    def fill_in_system_prompt(self, df):
        exchange_id = df['exchange_id'].tolist()
        system_prompt = []
        for i in exchange_id:
            if i in agent_logs:
                system_prompt.append(agent_logs[i])
            else:
                system_prompt.append("")
                
        df['system_prompt'] = system_prompt
        return df
        
    
    def get_data(self, query):
        db_config = {
        "host": "localhost",
        "database": "ohai_db",
        "user": "pol_data_user",
        "password": "serving6fir.oriole9SHIPWORM"
        }

        # Initialize and connect to the database
        db = PostgresDatabase(**db_config)
        db.connect()

        # SQL query to retrieve data
        
        # Fetch the data into a DataFrame
        df = db.fetch_data(query)

        # Disconnect from the database
        db.disconnect()
        
        return df
    
    
    def convert_timestamp(self, timestamp_str, target_timezone):
        # Parse the ISO8601 timestamp string to a datetime object with timezone info
        #original_time = datetime.fromisoformat(timestamp_str)
        
        # Set the timezone to UTC (since the original timestamp has "+00:00")
        #original_time = original_time.astimezone(pytz.utc)
        
        # Convert the time to the target timezone
        target_tz = pytz.timezone(target_timezone)
        converted_time = timestamp_str.astimezone(target_tz)
        
        # Format the datetime to the desired format
        formatted_time = converted_time.strftime('%Y-%m-%d %I:%M:%S %p')
        
        return formatted_time
    
    def convert_timestamp_from_str(self, timestamp_str, target_timezone):
        # Parse the ISO8601 timestamp string to a datetime object with timezone info
        original_time = datetime.fromisoformat(timestamp_str)
        
        # Set the timezone to UTC (since the original timestamp has "+00:00")
        original_time = original_time.astimezone(pytz.utc)
        
        # Convert the time to the target timezone
        target_tz = pytz.timezone(target_timezone)
        converted_time = original_time.astimezone(target_tz)
        
        # Format the datetime to the desired format
        formatted_time = converted_time.strftime('%Y-%m-%d %I:%M:%S %p')
        
        return formatted_time
    
    def time(self, df):
        #df['exchange_date'] = df.apply(lambda row: self.convert_timestamp(row['exchange_date'], row['timezone']), axis=1)
        #df['current_user_message_date'] = df.apply(lambda row: self.convert_timestamp(row['current_user_message_date'], row['timezone']), axis=1)
        return df
    
    
    def ai_steps_convert(self, df):
        ai_steps_info = df['ai_steps_info'].tolist()
        timezone = df['timezone'].tolist()
        
        for i in range(0,len(ai_steps_info)):
            for j in range(0,len(ai_steps_info[i])):
                if ai_steps_info[i][j]['timestamp']:
                    ai_steps_info[i][j]['timestamp'] = self.convert_timestamp_from_str(ai_steps_info[i][j]['timestamp'], timezone[i])
        
        df['ai_steps_info'] = ai_steps_info
        return df
    
    def get_timezone(self, df):
        exchange_ids = tuple(df['exchange_id'].tolist())

        # Corrected SQL query
        query = f"""
            SELECT e.id as exchange_id, u.timezone
            FROM exchanges e
            INNER JOIN users u ON e.consumer_id = u.id
            WHERE e.id IN {exchange_ids}
        """
        conversation = self.get_data(query)
        df2 = pd.merge(df, conversation, on='exchange_id', how='outer')
        return df2

    def get_all_exchanges(self):
        query = """select id as exchange_id, category as exchange_category, consumer_id, created_at as exchange_date, rag_text, household_member_id
        from exchanges
        where (category = 'user_initiated' or category = 'ha_initiated' or category = 'bot_initiated')
        AND created_at > '2024-09-25 00:00:00'
        AND created_at < '2024-10-02 00:00:00'
        order by created_at desc"""

        conversation = self.get_data(query)
        conversation = conversation.rename(columns={'household_member_id': 'Household Member ID'})
        print(conversation.columns)
        return conversation
    
    def add_columns_to_none_ha(self, df):
        columns = ['plan_id', 'human_id','prompt', 'classifier_intent', 'corrected_intent', 'model', 'timezone', 'ai_steps_info', 'system_prompt',
                   'Name', 'Last Name', 'Email', 'Personal Assistant Email', 'Calendar Connected',
                    'Memo Pad', 'Children of the Household','Adults of the Household', 'orientation_progess', 'user_profile_text_summary']
        
        for i in columns:
            df[i] = None
            
        df['conversation_history_last_20_messages'] = [[] for _ in range(len(df))]
            
        default_value = [{'action': None, 'prompt': None, 'thought': None, 'timestamp': None, 'action_input': None}]
        df.loc[df['ai_steps_info'].isnull(), 'ai_steps_info'] = df.loc[df['ai_steps_info'].isnull(), 'ai_steps_info'].apply(lambda x: default_value)
        
        return df
        
    def bot_query(self, bot_df, ha_df):
        exchange_ids = tuple(bot_df['exchange_id'].tolist())
        query = f"""SELECT 
                e.id as exchange_id,
                m.id as message_id,
                m.task_id,
                m.conversation_id,
                m.intent, 
                e.ha_escalation,
                e.apology_type, 
                e.apology_sub_type, 
                m.created_at as current_user_message_date,
                m.message as response
                FROM 
                    exchanges e
                LEFT JOIN 
                    messages m ON e.id = m.exchange_id
                WHERE 
                    e.id IN {exchange_ids}
                    AND m.type = 'bot'
                """
        conversation = self.get_data(query)
        
        bot_df = pd.merge(bot_df, conversation, on='exchange_id', how='outer')
        bot_df = self.add_columns_to_none_ha(bot_df)
        
        bot_df = bot_df[['exchange_id', 'exchange_category', 'consumer_id', 'exchange_date',
       'rag_text', 'Household Member ID', 'message_id', 'task_id', 'plan_id',
       'conversation_id', 'human_id', 'ha_escalation', 'apology_type',
       'apology_sub_type', 'intent', 'current_user_message_date', 'prompt',
       'response', 'classifier_intent', 'corrected_intent', 'model',
       'ai_steps_info', 'timezone', 'conversation_history_last_20_messages', 'system_prompt', 'Name', 'Last Name',
       'Email', 'Personal Assistant Email', 'Calendar Connected', 'Memo Pad',
       'user_profile_text_summary', 'Children of the Household',
       'Adults of the Household', 'orientation_progess']]
        
        
        exchange_ids = tuple(ha_df['exchange_id'].tolist())
        query = f"""SELECT 
                e.id as exchange_id,
                m.id as message_id,
                m.task_id,
                m.conversation_id,
                m.intent, 
                e.ha_escalation,
                e.apology_type, 
                e.apology_sub_type, 
                m.created_at as current_user_message_date,
                m.message as response
                FROM 
                    exchanges e
                LEFT JOIN 
                    messages m ON e.id = m.exchange_id
                WHERE 
                    e.id IN {exchange_ids}
                    AND m.type = 'ha'
                """
        
        conversation = self.get_data(query)
        ha_df = pd.merge(ha_df, conversation, on='exchange_id', how='outer')
        ha_df = self.add_columns_to_none_ha(ha_df)
        
        ha_df = ha_df[['exchange_id', 'exchange_category', 'consumer_id', 'exchange_date',
       'rag_text', 'Household Member ID', 'message_id', 'task_id', 'plan_id',
       'conversation_id', 'human_id', 'ha_escalation', 'apology_type',
       'apology_sub_type', 'intent', 'current_user_message_date', 'prompt',
       'response', 'classifier_intent', 'corrected_intent', 'model',
       'ai_steps_info', 'timezone', 'conversation_history_last_20_messages', 'system_prompt', 'Name', 'Last Name',
       'Email', 'Personal Assistant Email', 'Calendar Connected', 'Memo Pad',
       'user_profile_text_summary', 'Children of the Household',
       'Adults of the Household', 'orientation_progess']]
        
        return bot_df, ha_df
    
    def divide(self, df):
        ha_df = df.loc[df['exchange_category'] == "ha_initiated"].reset_index(drop = True)
        bot_df = df.loc[df['exchange_category'] == "bot_initiated"].reset_index(drop = True)
        user_df = df.loc[df['exchange_category'] == "user_initiated"].reset_index(drop = True)
        return ha_df, bot_df, user_df
    
    def add_columns_to_none(self, df):
        columns = ['plan_id', 'human_id', 'classifier_intent', 'corrected_intent', 'model']
        
        for i in columns:
            df[i] = None
        
        return df
    
    def get_basic_info(self, df):
        exchange_ids = tuple(df['exchange_id'].tolist())
        # e.category as exchange_category, 
        # e.created_at as exchange_date, 
        
        query = f"""SELECT 
                e.id as exchange_id, 
                m.id as message_id, 
                m.task_id, 
                ap.id as plan_id, 
                m.conversation_id, 
                ap.human_id,
                e.ha_escalation, 
                e.apology_type, 
                e.apology_sub_type, 
                m.intent,
                m.created_at as current_user_message_date, 
                ap.prompt, 
                ap.response, 
                ap.classifier_intent, 
                ap.corrected_intent, 
                ap.model,
                jsonb_agg(
                    jsonb_build_object(
                        'prompt', ase.prompt,
                        'thought', ase.thought,
                        'action', ase.action,
                        'action_input', ase.action_input,
                        'timestamp', ase.created_at
                        
                    ) ORDER BY ase.created_at
                ) AS ai_steps_info
                FROM 
                    exchanges e
                LEFT JOIN 
                    messages m ON e.id = m.exchange_id
                INNER JOIN 
                    ai_plans ap ON m.id = ap.message_id
                LEFT JOIN 
                    ai_steps ase ON ap.id = ase.plan_id
                WHERE 
                    e.id IN {exchange_ids}
                GROUP BY 
                    e.id, m.id, m.consumer_id, m.task_id, ap.id;
                """ 
        
        
        conversation = self.get_data(query)
        conversation = conversation.drop_duplicates(subset=['exchange_id']).reset_index(drop=True)
        #conversation = conversation[conversation['exchange_id'].map(conversation['exchange_id'].value_counts()) == 1]
        #conversation = conversation.reset_index(drop = True)
        
        df2 = pd.merge(df, conversation, on='exchange_id', how='outer')
        default_value = [{'action': None, 'prompt': None, 'thought': None, 'timestamp': None, 'action_input': None}]
        df2.loc[df2['ai_steps_info'].isnull(), 'ai_steps_info'] = df2.loc[df2['ai_steps_info'].isnull(), 'ai_steps_info'].apply(lambda x: default_value)
        other_df = df2.loc[df2['plan_id'].isnull()].reset_index(drop = True)
        df2 = df2.loc[~df2['plan_id'].isnull()].reset_index(drop = True)
        
        
        other_df = other_df[['exchange_id', 'exchange_category', 'consumer_id',	'exchange_date', 'rag_text', 'Household Member ID', 'ai_steps_info' ]]
        exchange_ids = tuple(other_df['exchange_id'].tolist())
        
        query = f"""SELECT 
                e.id as exchange_id, 
                m.id as message_id, 
                m.task_id, 
                m.conversation_id, 
                e.ha_escalation, 
                e.apology_type, 
                e.apology_sub_type, 
                m.intent,
                m.created_at as current_user_message_date,
                m.message as prompt
                FROM 
                    exchanges e
                LEFT JOIN 
                    messages m ON e.id = m.exchange_id
                WHERE 
                    e.id IN {exchange_ids}
                    AND m.type = 'user'
                """ 
                
        conversation = self.get_data(query)
        
        other_df = pd.merge(other_df, conversation, on='exchange_id', how='outer')
        
        query = f"""SELECT 
                e.id as exchange_id,
                m.message as response
                FROM 
                    exchanges e
                LEFT JOIN 
                    messages m ON e.id = m.exchange_id
                WHERE 
                    e.id IN {exchange_ids}
                    AND m.type = 'bot'
                """ 
                
        conversation = self.get_data(query)
        other_df = pd.merge(other_df, conversation, on='exchange_id', how='outer')
        other_df = self.add_columns_to_none(other_df)
        other_df = other_df.drop_duplicates(subset=['exchange_id']).reset_index(drop=True)
        
        other_df = other_df[['exchange_id', 'exchange_category', 'consumer_id', 'exchange_date',
       'rag_text', 'Household Member ID', 'message_id', 'task_id', 'plan_id',
       'conversation_id', 'human_id', 'ha_escalation', 'apology_type',
       'apology_sub_type', 'intent', 'current_user_message_date', 'prompt',
       'response', 'classifier_intent', 'corrected_intent', 'model',
       'ai_steps_info']]
        del conversation, exchange_ids

        return df2, other_df
    
    def add_timestamp_message(self, text, timestamp, timezone):
        
        timestamp =  self.convert_timestamp_from_str(timestamp, timezone)
        message = f"[{timestamp}] {text}"
        return message

        
    def process_conversation_data(self, history, function_history, timezone):
        """
        Processes conversation history and corresponding function calls.

        Args:
            history (list): A list of conversation histories, where each conversation
                            contains messages between user, bot, or other types.
            function_history (list): A list of function calls corresponding to the conversation history.

        Returns:
            list: A processed list of conversation dictionaries.
        """
        ans_all = []

        # Iterate over all conversation histories
        for i in range(len(history)):
            ans = []

            # Iterate over the exchanges in each conversation history
            for j in range(len(history[i])):
                if history[i][j]['messages']:
                    exchanges = history[i][j]['messages']

                    # Process each exchange within the conversation
                    for exchange in exchanges:
                        dicte = {}

                        # Process user message
                        if exchange['type'] == "user":
                            dicte['content'] = self.add_timestamp_message(exchange['message'], exchange['timestamp'], timezone[i])
                            dicte['role'] = "user"
                            dicte["additional_kwargs"] = {}
                            ans.append(dicte)

                            # Check for associated function calls for user message
                            if function_history[i][j]['function_calls']:
                                for func in function_history[i][j]['function_calls']:
                                    dicte_func = {}
                                    dicte_func['content'] = func['thought']
                                    dicte_func['role'] = "function"
                                    dicte_func["additional_kwargs"] = {
                                        'name': func['action'],
                                        'input': func['action_input']
                                    }
                                    ans.append(dicte_func)

                        # Process bot message
                        elif exchange['type'] == "bot":
                            dicte['content'] = self.add_timestamp_message(exchange['message'], exchange['timestamp'], timezone[i])
                            dicte['role'] = "assistant"
                            dicte["additional_kwargs"] = {}
                            ans.append(dicte)

                        # Process 'ha' message
                        elif exchange['type'] == "ha":
                            dicte['content'] = self.add_timestamp_message(exchange['message'], exchange['timestamp'], timezone[i])
                            dicte['role'] = "ha"
                            dicte["additional_kwargs"] = {}
                            ans.append(dicte)

            ans_all.append(ans)

        return ans_all
    
    def get_past_conversation(self, df):
        
        exchange_ids = tuple(df['exchange_id'].tolist())
        
        query =  f"""WITH MessageDetails AS (
                    SELECT
                        b.id AS exchange_id,
                        b.category,
                        JSON_AGG(
                            JSON_BUILD_OBJECT('message', m.message, 'type', m.type, 'timestamp', m.created_at, 'bot_message_type', m.bot_message_type)
                            ORDER BY m.created_at
                        ) AS messages,
                        b.created_at
                    FROM
                        exchanges AS b
                    LEFT JOIN
                        messages AS m ON b.id = m.exchange_id
                    WHERE
                        b.category IN ('user_initiated', 'bot_initiated', 'ha_initiated')
                    GROUP BY
                        b.id
                    ORDER BY 
                        b.created_at asc
                    ),
                    FunctionDetails AS (
                        SELECT
                            b.id AS exchange_id,
                            b.category,
                            JSON_AGG(
                                JSON_BUILD_OBJECT('prompt', ase.prompt, 'thought', ase.thought, 'action', ase.action, 'action_input', ase.action_input,
                                'action_output', ase.action_output, 'timestamp', ase.created_at)
                                ORDER BY ase.created_at
                            ) AS function_calls,
                            b.created_at
                        FROM
                            exchanges AS b
                        LEFT JOIN
                            messages AS m ON b.id = m.exchange_id
                        LEFT JOIN
                            ai_plans as ap ON m.id = ap.message_id
                        INNER JOIN
                            ai_steps as ase on ap.id = ase.plan_id
                        WHERE
                            b.category IN ('user_initiated', 'bot_initiated', 'ha_initiated')
                        GROUP BY
                            b.id
                        ORDER BY 
                            b.created_at asc
                    ),
                    Last20Exchanges AS (
                        SELECT
                            a.id AS current_exchange_id,
                            b.id AS previous_exchange_id,
                            b.category AS previous_category,
                            b.created_at AS previous_created_at,
                            b.consumer_id AS previous_consumer_id,
                            ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.created_at DESC) AS rn
                        FROM
                            exchanges AS a
                        JOIN
                            exchanges AS b ON a.consumer_id = b.consumer_id
                            AND b.created_at < a.created_at
                        WHERE
                            b.category IN ('user_initiated', 'bot_initiated', 'ha_initiated') AND
                            a.id in {exchange_ids}
                    )
                SELECT
                    a.id AS exchange_id,
                    a.category AS current_category,
                    a.created_at AS current_created_at,
                    a.consumer_id AS current_consumer_id,
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'messages', md.messages
                        )
                        ORDER BY b.previous_created_at
                    ) AS conversation_history_last_20_messages_messages,
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'function_calls', fd.function_calls
                        )
                        ORDER BY b.previous_created_at
                    ) AS conversation_history_last_20_function_calls
                FROM
                    exchanges AS a
                LEFT JOIN
                    Last20Exchanges AS b ON a.id = b.current_exchange_id
                    AND b.rn <= 20
                LEFT JOIN
                    MessageDetails AS md ON b.previous_exchange_id = md.exchange_id
                LEFT JOIN
                    FunctionDetails AS fd ON b.previous_exchange_id = fd.exchange_id
                WHERE
                    a.id in {exchange_ids}
                GROUP BY
                    a.id, a.category, a.created_at, a.consumer_id
                ORDER BY
                    a.created_at;"""

        
        conversation = self.get_data(query)
        df2 = pd.merge(df, conversation, on='exchange_id', how='outer')
        history = df2['conversation_history_last_20_messages_messages'].tolist()
        function_history = df2['conversation_history_last_20_function_calls'].tolist()
        timezone = df2['timezone'].tolist()
        past_conversation = self.process_conversation_data(history, function_history, timezone)
        df2['conversation_history_last_20_messages'] = past_conversation
        #df2 = df2.drop(columns=['conversation_history_last_20_messages_messages', 'conversation_history_last_20_function_calls', 'current_created_at',
        #                      'current_consumer_id'])
        return df2
    
    def get_future_conversation(self, df):
        
    
        exchange_ids = tuple(item for sublist in df['future_exchanges_ha_intervention'] if sublist for item in sublist)
        
        
        query = f"""SELECT
            b.id AS exchange_id,
            JSON_AGG(
                JSON_BUILD_OBJECT('prompt', ase.prompt, 'thought', ase.thought, 'action', ase.action, 'action_input', ase.action_input,
                'action_output', ase.action_output, 'timestamp', ase.created_at)
                ORDER BY ase.created_at
            ) AS function_calls_future
        FROM
            exchanges AS b
        LEFT JOIN
            messages AS m ON b.id = m.exchange_id
        LEFT JOIN
            ai_plans as ap ON m.id = ap.message_id
        LEFT JOIN
            ai_steps as ase on ap.id = ase.plan_id
        WHERE
            b.id in {exchange_ids}
        GROUP BY
            b.id
        ORDER BY 
            b.created_at asc"""
            
        user_Extractor = UserDataExtractor()   
        conversation_future_function = user_Extractor.get_data(query)
        kane = conversation_future_function['function_calls_future'].tolist()

        ans_all = [
            {'function_calls': i[:-1] if i[0].get('prompt') and i[0].get('thought') and i[0].get('action') else None}
            for i in kane
        ]

        conversation_future_function['function_calls_future'] = ans_all


        query = f"""SELECT
            b.id AS exchange_id,
            JSON_AGG(
                JSON_BUILD_OBJECT('message', m.message, 'type', m.type, 'timestamp', m.created_at, 'bot_message_type', m.bot_message_type)
                ORDER BY m.created_at
            ) AS messages_future
        FROM
            exchanges AS b
        LEFT JOIN
            messages AS m ON b.id = m.exchange_id
        WHERE
            b.id in {exchange_ids}
        GROUP BY
            b.id
        ORDER BY 
            b.created_at asc"""
            
        user_Extractor = UserDataExtractor()   
        conversation_future_messages = user_Extractor.get_data(query)

        kane = conversation_future_messages['messages_future'].tolist()

        ans_all = [
            {'messages': i if i[0].get('message') and i[0].get('type') and i[0].get('timestamp') else None}
            for i in kane
        ]

        conversation_future_messages['messages_future'] = ans_all

        messages_future = []
        function_calls_future = []
        future_exchanges = df['future_exchanges_ha_intervention'].tolist()
        for i in future_exchanges:
            msg_f = []
            fun_f = []
            for j in i:
                df_try = conversation_future_function.loc[conversation_future_function['exchange_id'] == j]
                fun_f.append(df_try['function_calls_future'].tolist()[0])
                df_try = conversation_future_messages.loc[conversation_future_messages['exchange_id'] == j]
                msg_f.append(df_try['messages_future'].tolist()[0])
            messages_future.append(msg_f)
            function_calls_future.append(fun_f)

        df['messages_future'] = messages_future
        df['function_calls_future'] = function_calls_future
   
        history = df['messages_future'].tolist()
        function_history = df['function_calls_future'].tolist()
        timezone = df['timezone'].tolist()
        past_conversation = self.process_conversation_data(history, function_history, timezone)
        df['conversation_history_ha_intervention'] = past_conversation
        #df2 = df2.drop(columns=['conversation_history_last_20_messages_messages', 'conversation_history_last_20_function_calls', 'current_created_at',
        #                      'current_consumer_id'])
        return df
    
    def get_current_session_id(self, df):
        exchange_ids = tuple(df['exchange_id'].tolist())
        query = f"""SELECT id AS exchange_id, session_id as current_session_id
                FROM exchanges
                WHERE id IN {exchange_ids}"""
        conversation = self.get_data(query)
        df2 = pd.merge(df, conversation, on='exchange_id', how='outer')
        
        return df2
    

    def get_exchange_for_current_session(self, df):
        # Step 1: Get all exchanges in the relevant sessions
        current_session_ids = tuple(df['current_session_id'].tolist())
        
        # Query to get all exchanges for the current session ids
        query = f"""
        SELECT id as exchange_id, session_id, created_at, category
        FROM exchanges
        WHERE session_id IN {current_session_ids}
        """
        
        # Assuming self.get_data(query) returns a DataFrame
        session_exchanges = self.get_data(query)

        # Step 2: Prepare a new column for subsequent exchanges
        # Initialize an empty list to hold the subsequent exchanges for each row
        subsequent_exchange_ids = []

        # Iterate over each row of the original DataFrame
        for index, row in df.iterrows():
            # Extract the current session_id and created_at of the exchange
            current_session_id = row['current_session_id']
            current_created_at = row['exchange_date']  # Assuming exchange_date is the created_at timestamp
            
            # Filter session_exchanges to get only those in the same session and created later
            filtered_exchanges = session_exchanges[
                (session_exchanges['session_id'] == current_session_id) &
                (session_exchanges['created_at'] > current_created_at)
            ]

            # Sort the filtered exchanges by created_at to get them in the correct order
            sorted_exchanges = filtered_exchanges.sort_values(by='created_at')

            # Get the sorted list of exchange_ids
            subsequent_exchanges = sorted_exchanges['exchange_id'].tolist()
            subsequent_category = sorted_exchanges['category'].tolist()

            # Append the list of subsequent exchanges
            subsequent_exchange_ids.append({"exchange_id": subsequent_exchanges, 'category': subsequent_category})

        # Add the new column to the original DataFrame
        df['subsequent_exchanges'] = subsequent_exchange_ids

        return df
    
    def get_exchange_for_next_session_prev(self, df):
        # Step 1: Get all unique consumer_ids and their associated session and exchange data
        consumer_ids = tuple(df['consumer_id'].unique())
        
        # Query to get all exchanges for these consumer_ids
        query = f"""
        SELECT id as exchange_id, session_id, created_at, consumer_id, category
        FROM exchanges
        WHERE consumer_id IN {consumer_ids}
        """

        # Assuming self.get_data(query) returns a DataFrame
        consumer_exchanges = self.get_data(query)

        # Sort the DataFrame for easier processing
        consumer_exchanges = consumer_exchanges.sort_values(by=['consumer_id', 'created_at'])

        # Step 2: Prepare a new column for subsequent exchanges in the next session
        next_session_exchange_ids = []

        # Iterate over each row of the original DataFrame
        for index, row in df.iterrows():
            current_consumer_id = row['consumer_id']
            current_session_id = row['current_session_id']
            #current_created_at = row['created_at']  # Using created_at timestamp

            # Filter exchanges for the current consumer
            consumer_data = consumer_exchanges[consumer_exchanges['consumer_id'] == current_consumer_id]

            # Find the unique session_ids in order for the consumer
            session_order = consumer_data['session_id'].unique()
            current_session_index = list(session_order).index(current_session_id)
            
            # Check if there is a next session for this consumer
            if current_session_index < len(session_order) - 1:
                next_session_id = session_order[current_session_index + 1]

                # Get all exchanges in the next session
                next_session_exchanges = consumer_data[
                    (consumer_data['session_id'] == next_session_id)
                ]

                # Get the sorted list of exchange_ids from the next session
                next_session_exchange_list = next_session_exchanges['exchange_id'].tolist()
                next_session_category = next_session_exchanges['category'].tolist()

                # Append the list of exchange_ids
                #next_session_exchange_ids.append(next_session_exchange_list)
                next_session_exchange_ids.append({"exchange_id": next_session_exchange_list[0], 'category': next_session_category[0]})
            else:
                # If there is no next session, append an empty list
                next_session_exchange_ids.append({"exchange_id": [], 'category': []})

        # Add the new column to the original DataFrame
        df['subsequent_exchanges_next_session'] = next_session_exchange_ids

        return df
    
    
    def get_exchange_for_next_session(self, df):
        # Step 1: Get all unique consumer_ids and their associated session and exchange data
        consumer_ids = tuple(df['consumer_id'].unique())
        
        # Query to get all exchanges for these consumer_ids
        query = f"""
        SELECT id as exchange_id, session_id, created_at, consumer_id, category
        FROM exchanges
        WHERE consumer_id IN {consumer_ids}
        """
        
        # Assuming self.get_data(query) returns a DataFrame
        consumer_exchanges = self.get_data(query)
        
        # Sort the consumer_exchanges DataFrame for easier processing
        consumer_exchanges = consumer_exchanges.sort_values(by=['consumer_id', 'created_at'])
        
        # Step 2: Find the next session for each exchange
        # Create a mapping of consumer_id to session_id order
        #consumer_exchanges['session_rank'] = consumer_exchanges.groupby('consumer_id')['session_id'].rank(method='dense').astype(int)
        consumer_exchanges['session_rank'] = consumer_exchanges.groupby('consumer_id')['session_id'].rank(method='dense').fillna(0).astype(int)

        
        # Create a DataFrame with only the first exchange in the next session for each consumer
        consumer_exchanges['next_session_rank'] = consumer_exchanges['session_rank'] + 1
    
        # Prepare a DataFrame that has each session and the first exchange of the subsequent session
        next_sessions = consumer_exchanges.merge(
            consumer_exchanges[['consumer_id', 'session_rank', 'exchange_id', 'category']],
            left_on=['consumer_id', 'next_session_rank'],
            right_on=['consumer_id', 'session_rank'],
            suffixes=('', '_next'),
            how='left'
        )[['consumer_id', 'session_id', 'exchange_id_next', 'category_next']]
        
        # Rename columns to make the next session exchange details clearer
        next_sessions.rename(columns={'exchange_id_next': 'next_exchange_id', 'category_next': 'next_category'}, inplace=True)
        
        # Step 3: Merge with original DataFrame to get next session details
        df = df.merge(
            next_sessions,
            left_on=['consumer_id', 'current_session_id'],
            right_on=['consumer_id', 'session_id'],
            how='left'
        )
        
        # Prepare the final column as dictionaries of exchange_id and category
        df['subsequent_exchanges_next_session'] = df.apply(
            lambda x: {"exchange_id": x['next_exchange_id'], "category": x['next_category']}
            if pd.notna(x['next_exchange_id']) else {"exchange_id": [], "category": []},
            axis=1
        )
        
        # Drop intermediate columns
        df.drop(columns=['session_id', 'next_exchange_id', 'next_category'], inplace=True)
        
        df = df.drop_duplicates(subset=['exchange_id'], keep='first').reset_index(drop=True)

        return df
    
    
    def get_exchange_ids_final(self, df):
        # Extracting lists from DataFrame for current and next session exchanges
        same_session = df['subsequent_exchanges'].tolist()
        next_session = df['subsequent_exchanges_next_session'].tolist()

        ans_all = []

        # Iterating through each element of the sessions to determine valid exchanges
        for i in range(len(same_session)):
            # Extract category and exchange_id from the same session
            category = same_session[i]['category']
            exchange_id = same_session[i]['exchange_id']

            ans = []
            ha_initiated_found = False

            # Iterate through each category to determine if "ha_initiated" exists
            for j in range(len(category)):
                if category[j] == "ha_initiated":
                    # If "ha_initiated" is found, add the corresponding exchange_id and exit loop
                    ans.append(exchange_id[j])
                    ha_initiated_found = True
                    break
                elif category[j] != "email_initiated":
                    # For other categories, except "email_initiated", add exchange_id
                    ans.append(exchange_id[j])

            # If "ha_initiated" was not found in the same session, check the next session
            if not ha_initiated_found:
                if next_session[i]['category'] == "ha_initiated":
                    ans.append(next_session[i]['exchange_id'])
                else:
                    ans = []

            # Append the result for the current row to the overall answer list
            ans_all.append(ans)

        # Add the new list as a column in the DataFrame
        df['future_exchanges_ha_intervention'] = ans_all

        return df


# Example usage of the UserDataExtractor class
if __name__ == "__main__":
    #df_gh = extract_s3_data()
    user_Extractor = UserDataExtractor()
    main_df = user_Extractor.get_all_exchanges()
    print("got all exchanges")
    print(main_df.shape)
    print("\n")
    ha_df, bot_df, user_df = user_Extractor.divide(main_df)
    
    print("user exchanges")
    print(user_df.shape)
    print("\n")
    enriched_df, other_df = user_Extractor.get_basic_info(user_df)
    enriched_df = enriched_df.dropna(subset=['consumer_id'])
    print("got basic info")
    print(enriched_df.shape)
    print("\n")
    print(other_df.shape)
    print("\n")
    enriched_df = pd.concat([enriched_df, other_df], ignore_index=True)
    print("got basic info final")
    print(enriched_df.shape)
    print(enriched_df.columns)
    print("\n")
    enriched_df = user_Extractor.get_timezone(enriched_df)
    print("got timezone")
    print(enriched_df.shape)
    print("\n")
    enriched_df = user_Extractor.fill_in_system_prompt(enriched_df)
    print("filled system prompt")
    print(enriched_df.shape)
    print("\n")
    enriched_df = user_Extractor.enrich_dataframe_with_extracted_info(enriched_df)
    enriched_df = user_Extractor.ai_steps_convert(enriched_df)
    bot_df, ha_df = user_Extractor.bot_query(bot_df, ha_df)
    print("bot and ha query done")
    print(bot_df.shape)
    print(ha_df.shape)
    print("\n")
    bot_ha_df = pd.concat([bot_df, ha_df ], ignore_index=True)
    """
    enriched_df = user_Extractor.get_current_session_id(enriched_df)
    print("got current session id")
    print(enriched_df.shape)
    print("\n")
    enriched_df = user_Extractor.get_exchange_for_current_session(enriched_df)
    print("got exchange for current session")
    print(enriched_df.shape)
    print("\n")
    enriched_df = user_Extractor.get_exchange_for_next_session(enriched_df)
    print("got exchange for next session")
    print(enriched_df.shape)
    print("\n")
    enriched_df = user_Extractor.get_exchange_ids_final(enriched_df)
    print("got future exchange ids final")
    print(enriched_df.shape)
    print("\n")
    enriched_df = user_Extractor.get_future_conversation(enriched_df)
    print("got future history")
    print(enriched_df.shape)
    print("\n")
    """
    #enriched_df = user_Extractor.time(enriched_df)
    #enriched_df['conversation_history_last_20_messages'] = [[] for _ in range(len(enriched_df))]
    enriched_df = user_Extractor.get_past_conversation(enriched_df)
    #'system_prompt', 'timezone'
    #'conversation_history_last_20_messages'
    # 'conversation_history_last_20_messages', 'conversation_history_last_20_messages_messages', 'conversation_history_last_20_function_calls',
    
    enriched_df = enriched_df[['exchange_id', 'exchange_category', 'consumer_id', 'exchange_date',
       'rag_text', 'Household Member ID', 'message_id', 'task_id', 'plan_id',
       'conversation_id', 'human_id', 'ha_escalation', 'apology_type',
       'apology_sub_type', 'intent', 'current_user_message_date', 'prompt',
       'response', 'classifier_intent', 'corrected_intent', 'model',
       'ai_steps_info', 'timezone', 'conversation_history_last_20_messages', 'system_prompt', 'Name', 'Last Name',
       'Email', 'Personal Assistant Email', 'Calendar Connected', 'Memo Pad', 'user_profile_text_summary', 'Children of the Household',
       'Adults of the Household', 'orientation_progess']]
    enriched_df = pd.concat([enriched_df, bot_ha_df], ignore_index=True)
   
    #j
    
  