import pandas as pd
import json
import warnings
from markdownify import markdownify as md
from bs4 import MarkupResemblesLocatorWarning

# Suppress BeautifulSoup warnings
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

def load_connections_metadata(file_path: str) -> dict:
    """
    Loads LinkedIn Connections CSV file and returns a metadata dictionary.
    Matches names to their Company and Position.
    """
    try:
        # Based on the notebook, the header is on line 3 (index)
        df = pd.read_csv(file_path, sep=',', skiprows=3, header=0)
        
        # Strip whitespace from column names
        df.columns = df.columns.str.strip()
        
        # Create 'full_name' column, handling potential NaNs and stripping whitespace
        df['First Name'] = df['First Name'].astype(str).str.strip()
        df['Last Name'] = df['Last Name'].astype(str).str.strip()
        df['full_name'] = df['First Name'] + ' ' + df['Last Name']
        df['full_name'] = df['full_name'].str.replace('nan nan', '').str.strip()
        
        # Create connections_metadata dictionary
        # Mapping full_name -> {Company: ..., Position: ...}
        connections_metadata = (
            df.set_index('full_name')[['Company', 'Position']]
            .apply(lambda x: x.dropna().to_dict(), axis=1)
            .to_dict()
        )
        return connections_metadata
    except Exception as e:
        print(f"Error loading connections: {e}")
        return {}

def load_messages_data(file_path: str) -> pd.DataFrame:
    """
    Loads LinkedIn Messages CSV file and returns a DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        
        # Standardize 'FROM' and 'TO' columns by stripping whitespace
        df['FROM'] = df['FROM'].astype(str).str.strip()
        df['TO'] = df['TO'].astype(str).str.strip()
        
        # Convert 'DATE' column to datetime objects
        df['DATE'] = pd.to_datetime(df['DATE'], utc=True)
        
        return df
    except Exception as e:
        print(f"Error loading messages: {e}")
        return pd.DataFrame()

def consolidate_linkedin_data(connections_metadata: dict, df_messages: pd.DataFrame) -> list:
    """
    Consolidates connections and messages into a denormalized conversation structure.
    """
    denormalized_conversations = []
    
    # Group messages by conversation ID
    for conversation_id, conversation_group in df_messages.groupby('CONVERSATION ID'):
        # Identify unique participants
        senders = conversation_group['FROM'].unique().tolist()
        recipients = conversation_group['TO'].unique().tolist()
        all_participants_names = list(set(senders + recipients))
        
        participants_data = []
        for participant_name in all_participants_names:
            # Retrieve metadata for each participant
            # metadata will be a dict with Company and Position if they exist
            metadata = connections_metadata.get(participant_name, {})
            
            p_entry = {'full_name': participant_name}
            if metadata.get('Company') and metadata['Company'] != 'Unknown':
                p_entry['Company'] = metadata['Company']
            if metadata.get('Position') and metadata['Position'] != 'Unknown':
                p_entry['Position'] = metadata['Position']
            
            participants_data.append(p_entry)
            
        # Prepare message objects for the conversation
        messages = []
        # Sort by date to ensure chronological order
        for _, row in conversation_group.sort_values(by='DATE').iterrows():
            msg_content = row['CONTENT']
            
            # Convert HTML to Markdown
            if pd.isna(msg_content):
                msg_content = ''
            else:
                msg_content = str(msg_content)
                if msg_content:
                    msg_content = md(msg_content, strip=['a'], newline_style='backslash')
            
            messages.append({
                'sender': row['FROM'],
                'timestamp': row['DATE'].isoformat(),
                'content': msg_content
            })
            
        # Assemble the conversation dictionary
        conversation_dict = {
            'conversation_id': conversation_id,
            'participants': participants_data,
            'messages': messages
        }
        denormalized_conversations.append(conversation_dict)
        
    return denormalized_conversations

def save_to_json(data: list, output_path: str) -> None:
    """
    Saves the list of conversations to a JSON file.
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Successfully saved consolidated data to {output_path}")
    except Exception as e:
        print(f"Error saving to JSON: {e}")

def run_consolidation(connections_csv: str, messages_csv: str, output_json: str) -> None:
    """
    Utility function to run the full consolidation pipeline.
    """
    print("Loading connections metadata...")
    connections_metadata = load_connections_metadata(connections_csv)
    
    print("Loading messages data...")
    df_messages = load_messages_data(messages_csv)
    
    if df_messages.empty:
        print("No messages found to consolidate.")
        return
    
    print("Consolidating data...")
    consolidated_data = consolidate_linkedin_data(connections_metadata, df_messages)
    
    print(f"Processed {len(consolidated_data)} conversations.")
    
    save_to_json(consolidated_data, output_json)

def run_from_cli():
    import argparse
    parser = argparse.ArgumentParser(description='Consolidate LinkedIn account export data.')
    parser.add_argument('--connections', type=str, help='Path to Connections.csv')
    parser.add_argument('--messages', type=str, help='Path to messages.csv')
    parser.add_argument('--output', type=str, help='Path to output JSON file')
    
    args = parser.parse_args()
    
    if args.connections and args.messages and args.output:
        run_consolidation(args.connections, args.messages, args.output)
    else:
        print("Please provide --connections, --messages, and --output arguments.")

if __name__ == "__main__":
    run_from_cli()
