import os
import asyncio
import pandas as pd
import threading
import sqlite3
import shutil
from flask import Flask, request, render_template, flash, redirect, url_for, session, jsonify
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError, PhoneNumberInvalidError
from telethon.tl.types import InputPeerUser
import secrets  # For secure session naming

app = Flask(__name__)
app.secret_key = os.environ.get('yGfqXco3Ci6+yYNZRs26pmu06uVyTUSAZsPzuzZ5li7sDxosksKN4hstpVe/jw/9Os1QjuHdxhI8rnjSxwBVCg==', secrets.token_hex(16))

# Get credentials from environment variables (get from my.telegram.org)
API_ID_STR = os.environ.get('25509235', '').strip()
API_HASH_STR = os.environ.get('d3629ab967e8ecac197831192aa36d65', '').strip()

# Store credentials globally, will be set when available
API_ID = 25509235
API_HASH = 'd3629ab967e8ecac197831192aa36d65'

@app.route('/')
def index():
    # redirect authenticated users directly to dashboard
    if auth_state.get('is_authenticated'):
        return redirect(url_for('dashboard'))
    return render_template('index.html', 
                         authenticated=auth_state['is_authenticated'],
                         code_requested=auth_state['code_requested'],
                         phone_number=auth_state['phone_number'])

# Phone number will be provided by user

# Authentication state (no global client)
auth_state = {
    'code_requested': False,
    'is_authenticated': False,
    'phone_code_hash': None,
    'phone_number': None,
    'session_string': None,  # Temporary StringSession during auth flow
    'monitoring_session_string': None  # Final StringSession for monitoring to avoid DB conflicts
}

# Global state for message sending control
sending_state = {
    'is_sending': False,
    'should_stop': False,
    'current_message': 0,
    'total_messages': 0,
    'current_number': '',
    'messages_sent_successfully': 0,
    'messages_failed': 0,
    'start_time': None,
    'estimated_time_remaining': 0,
    'current_recipient': '',
    'send_mode': '',
    'last_message_sent': '',
    'sending_speed': 0,  # messages per minute
    'is_paused': False,  # whether currently in a 2-minute pause
    'pause_countdown': 0,  # seconds remaining in pause
    'error_message': None,  # error message if sending fails
    'skip_count': 0  # number of messages to skip at the beginning
}

# State for reply monitoring - optimized for current batch processing
reply_state = {
    'monitoring': False,
    'target_recipient': None,  # Specific recipient to monitor for duplicates (username or user_id)
    'target_groups': [],  # List of group IDs/names to search for matching numbers (empty = search all groups)
    'found_matches': {},  # {recipient_id: {pattern: number}} - patterns already processed per recipient
    'group_numbers': {},  # {number: [{'peer_id': id, 'access_hash': hash, 'msg_id': id, 'pattern': pattern}]}
    'processed_messages': set(),  # Track processed (peer_id, msg_id) tuples to avoid reprocessing
    'replies_received': {},  # {recipient_id: [list of reply messages]} - track replies per recipient
    'duplicate_replies': {},  # {recipient_id: {number: count}} - track duplicate counts per recipient
    'sending_start_times': {},  # {recipient_id: timestamp} - when sending started to each recipient
    'duplicate_time_window': 1800,  # Time window in seconds (30 minutes) for duplicate detection
    'number_timestamps': {},  # {recipient_id: {number: [timestamps]}} - track when numbers were received
    'last_auto_reply': {},  # {recipient_id: {number: timestamp}} - prevent spam by tracking last auto-reply sent
    'group_numbers_ttl': {},  # {number: timestamp} - TTL for group search data to prevent stale matches
    'lifetime_duplicate_count': {},  # {recipient_id: {number: count}} - total duplicates ever seen from recipient (never resets)
    'pending_searches': {},  # {(recipient_id, pattern): {'task': task, 'start_time': timestamp, 'reply_time': timestamp, 'status': 'searching'|'found'|'timeout'}}
    'reply_timestamps': {}  # {recipient_id: {number: timestamp}} - when each reply was received from recipient
}

# Global locks for thread safety
telegram_lock = threading.Lock()  # For Telegram client access
reply_state_lock = threading.Lock()  # For reply_state mutations

# Add a proper monitoring control system
monitoring_thread = None
monitoring_stop_event = threading.Event()
monitoring_client = None

# UI moved to templates and static files; CSS/JS handled separately
@app.route('/request_code', methods=['POST'])
def request_code():
    """Request authentication code from Telegram"""
    phone = request.form['phone'].strip()
    
    if not phone:
        flash('Please enter a phone number.', 'error')
        return redirect(url_for('index'))
    
    # Normalize phone number (remove spaces, dashes, etc. but keep +)
    normalized_phone = phone.replace(' ', '').replace('-', '').replace('(', '').replace(')', '').replace('.', '')
    
    # Store normalized phone number in auth state
    auth_state['phone_number'] = normalized_phone
    
    try:
        async def _request_code():
            # Check if API credentials are available
            if not API_ID or not API_HASH:
                return {'success': False, 'error': 'Telegram API credentials not configured. Please contact administrator.'}
            
            # Use normalized phone number for validation
            phone_to_use = normalized_phone
            
            # Validate phone number format
            if not phone_to_use.startswith('+'):
                return {'success': False, 'error': 'Phone number must start with + and include country code (e.g., +12345678901)'}
            
            # Basic validation - should be + followed by 7-15 digits
            phone_digits = phone_to_use[1:]  # Remove + sign
            if not phone_digits.isdigit() or len(phone_digits) < 7 or len(phone_digits) > 15:
                return {'success': False, 'error': 'Please enter a valid phone number with country code (7-15 digits after +)'}
            
            # Create client with StringSession for monitoring compatibility
            client = TelegramClient(StringSession(), API_ID, API_HASH)
            
            try:
                # Connect manually without triggering interactive start
                await client.connect()
                
                if not await client.is_user_authorized():
                    result = await client.send_code_request(phone_to_use)
                    auth_state['phone_code_hash'] = result.phone_code_hash
                    auth_state['code_requested'] = True
                    # Save the StringSession for use in login
                    if hasattr(client, 'session') and client.session:
                        auth_state['session_string'] = client.session.save()
                    else:
                        return {'success': False, 'error': 'Failed to create session. Please try again.'}
                    return {'success': True, 'code_requested': True}
                else:
                    # User is already authorized, save session for monitoring
                    auth_state['is_authenticated'] = True
                    if hasattr(client, 'session') and client.session:
                        session_string = client.session.save()
                        auth_state['monitoring_session_string'] = session_string
                        print(f"User already authorized, saved session for monitoring (length: {len(session_string)})")
                    else:
                        print("Warning: Could not save session for monitoring")
                    return {'success': True, 'code_requested': False}
            except PhoneNumberInvalidError:
                return {'success': False, 'error': 'This phone number is not valid. Please enter a real phone number with country code (e.g., +1234567890).'}
            except FloodWaitError as e:
                return {'success': False, 'error': f'Rate limited by Telegram. Please wait {e.seconds} seconds and try again.'}
            except Exception as e:
                # Print actual error for debugging
                print(f"Telegram API Error: {type(e).__name__}: {str(e)}")
                # More detailed error message
                error_msg = f"Failed to request login code: {str(e)}. Please check your phone number and try again."
                return {'success': False, 'error': error_msg}
            finally:
                try:
                    if client.is_connected():
                        result = client.disconnect()
                        if result is not None:
                            await result
                except Exception:
                    pass  # Ignore disconnect errors
        
        result = asyncio.run(_request_code())
        
        if result['success']:
            if result['code_requested']:
                flash('Login code sent to your Telegram app!', 'success')
            else:
                flash('Already authenticated!', 'success')
        else:
            flash(f'Error requesting code: {result["error"]}', 'error')
            auth_state['code_requested'] = False  # Reset on error
    except Exception as e:
        flash(f'Unexpected error: {str(e)}', 'error')
        auth_state['code_requested'] = False  # Reset on error
    
    return redirect(url_for('index'))

@app.route('/login', methods=['POST'])
def login():
    """Complete authentication with code from Telegram"""
    code = request.form['code']
    try:
        async def _login():
            phone = auth_state.get('phone_number')
            if not phone:
                return 'error:Phone number not found. Please start over.'
            
            # Check if API credentials are available
            if not API_ID or not API_HASH:
                return 'error:Telegram API credentials not configured. Please contact administrator.'
            
            # Use the StringSession that was created during code request
            session_string = auth_state.get('session_string')
            if not session_string:
                return 'error:Session not found. Please restart the process.'
            
            client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
            try:
                await client.connect()
                phone_code_hash = auth_state.get('phone_code_hash')
                if not phone_code_hash:
                    return 'error:Phone code hash not found. Please restart the process.'
                
                # Clean the code (remove spaces, dashes, etc.)
                clean_code = ''.join(filter(str.isdigit, code))
                if len(clean_code) != 5:
                    return 'error:Code must be exactly 5 digits. Please check and try again.'
                
                # Sign in with the StringSession
                await client.sign_in(phone, clean_code, phone_code_hash=phone_code_hash)
                me = await client.get_me()
                name = getattr(me, 'first_name', 'User')
                
                # Save the authenticated StringSession for monitoring
                if hasattr(client, 'session') and client.session:
                    auth_state['monitoring_session_string'] = client.session.save()
                else:
                    print("Warning: Could not save session for monitoring")
                    auth_state['monitoring_session_string'] = None
                auth_state['is_authenticated'] = True
                
                # Clear temporary auth data
                auth_state['phone_code_hash'] = None
                auth_state['code_requested'] = False
                auth_state['session_string'] = None
                
                session_length = len(auth_state['monitoring_session_string']) if auth_state['monitoring_session_string'] else 0
                print(f"Successfully authenticated and saved StringSession for monitoring (length: {session_length})")
                
                return f'Logged in as {name}!'
            except SessionPasswordNeededError:
                return 'error:2FA password needed. Please disable 2FA or extend application to handle it.'
            except PhoneCodeInvalidError:
                return 'error:Invalid code. Please check the code from your Telegram app and try again.'
            except Exception as e:
                return f'error:Authentication failed: {str(e)}'
            finally:
                try:
                    if hasattr(client, 'is_connected') and client.is_connected():
                        disconnect_result = client.disconnect()
                        if disconnect_result is not None:
                            await disconnect_result
                except:
                    pass  # Ignore disconnect errors
        
        result = asyncio.run(_login())
        
        if result.startswith('error:'):
            flash(result[6:], 'error')
            return redirect(url_for('index'))
        else:
            flash(result, 'success')
            # Set monitoring as always active immediately upon login
            reply_state['monitoring'] = True
            # Auto-start reply monitoring after successful login
            auto_start_monitoring()
            # Start watchdog to ensure monitoring stays active
            start_monitoring_watchdog()
            return redirect(url_for('dashboard'))
    except Exception as e:
        flash(f'Login error: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/dashboard', methods=['GET'])
def dashboard():
    """Dashboard page for authenticated users"""
    if not auth_state['is_authenticated']:
        flash('Please login first.', 'error')
        return redirect(url_for('index'))
    # render separate template now that UI has been moved
    return render_template('dashboard.html')
    

@app.route('/logout', methods=['GET'])
def logout():
    """Logout and clear authentication state"""
    # Force logout by clearing session files
    import glob
    try:
        # Remove all session files to force complete logout
        session_files = glob.glob('session_*.session') + glob.glob('web_session.session') + glob.glob('*_send.session') + glob.glob('*_monitor.session')
        for session_file in session_files:
            try:
                os.remove(session_file)
                print(f"Removed session file: {session_file}")
            except FileNotFoundError:
                pass  # File already deleted
            except PermissionError:
                print(f"Permission denied removing {session_file}")
    except Exception as e:
        print(f"Error during session cleanup: {e}")
    
    # Clear authentication state
    auth_state['is_authenticated'] = False
    auth_state['code_requested'] = False
    auth_state['phone_code_hash'] = None
    auth_state['phone_number'] = None
    auth_state['session_name'] = None
    
    # Stop any ongoing operations
    sending_state['should_stop'] = True
    sending_state['is_sending'] = False
    reply_state['monitoring'] = False
    
    flash('Force logout completed! All sessions cleared.', 'success')
    return redirect(url_for('index'))

@app.route('/stop', methods=['POST'])
def stop_sending():
    """Stop the current message sending process"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    sending_state['should_stop'] = True
    return jsonify({'status': 'success', 'message': 'Stop signal sent'})

@app.route('/sending_status', methods=['GET'])
def get_sending_status():
    """Get current sending status"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    # Calculate estimated time remaining and sending speed
    if sending_state['start_time'] and sending_state['current_message'] > 0:
        import time
        elapsed_time = time.time() - sending_state['start_time']
        if elapsed_time > 0:
            sending_state['sending_speed'] = round((sending_state['current_message'] / elapsed_time) * 60, 1)  # messages per minute
            remaining_messages = sending_state['total_messages'] - sending_state['current_message']
            if sending_state['sending_speed'] > 0:
                sending_state['estimated_time_remaining'] = int(round((remaining_messages / sending_state['sending_speed']) * 60))  # ensure integer seconds
    
    return jsonify({
        'is_sending': sending_state['is_sending'],
        'should_stop': sending_state['should_stop'],
        'current_message': sending_state['current_message'],
        'total_messages': sending_state['total_messages'],
        'current_number': sending_state['current_number'],
        'messages_sent_successfully': sending_state['messages_sent_successfully'],
        'messages_failed': sending_state['messages_failed'],
        'estimated_time_remaining': sending_state['estimated_time_remaining'],
        'current_recipient': sending_state['current_recipient'],
        'send_mode': sending_state['send_mode'],
        'last_message_sent': sending_state['last_message_sent'],
        'sending_speed': sending_state['sending_speed'],
        'is_paused': sending_state['is_paused'],
        'pause_countdown': sending_state['pause_countdown'],
        'error_message': sending_state['error_message']
    })

@app.route('/upload', methods=['POST'])
def upload():
    """Handle CSV upload or manual data input and send to Telegram recipient"""
    if not auth_state['is_authenticated']:
        # Check if this is an AJAX request
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or 'application/json' in request.headers.get('Accept', ''):
            return jsonify({'status': 'error', 'message': 'Please login first.'}), 401
        flash('Please login first.', 'error')
        return redirect(url_for('index'))
    
    file = request.files.get('file')
    manual_data = request.form.get('manual_data', '').strip()
    recipient = request.form['recipient'].strip()
    send_mode = request.form.get('send_mode', 'columns').strip()  # Get send mode
    skip_count = int(request.form.get('skip_count', 0))  # Get skip count
    
    # Validate input: Ensure either file or manual data is provided
    if not (file and file.filename) and not manual_data:
        # Check if this is an AJAX request
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or 'application/json' in request.headers.get('Accept', ''):
            return jsonify({'status': 'error', 'message': 'Please provide either a file (XLSX, CSV, or TXT) or manual data.'}), 400
        flash('Please provide either a file (XLSX, CSV, or TXT) or manual data.', 'error')
        return redirect(url_for('dashboard'))
    
    # Read file data NOW before request context ends (Flask closes streams when request ends)
    file_payload = None
    if file and file.filename:
        file_payload = {
            'filename': file.filename,
            'data': file.read()  # Read all data into memory before background thread starts
        }
    
    # Initialize sending state immediately before starting background thread
    import time
    sending_state['should_stop'] = False
    sending_state['is_sending'] = True
    sending_state['current_message'] = 0
    sending_state['total_messages'] = 0  # Will be updated once data is processed
    sending_state['messages_sent_successfully'] = 0
    sending_state['messages_failed'] = 0
    sending_state['start_time'] = time.time()
    sending_state['current_recipient'] = recipient
    sending_state['send_mode'] = send_mode
    sending_state['error_message'] = None
    sending_state['skip_count'] = skip_count  # Store skip count
    
    # Automatically set this recipient as the monitoring target
    clean_recipient = recipient.lstrip('@')
    if reply_state['target_recipient'] != clean_recipient:
        reply_state['target_recipient'] = clean_recipient
        print(f"Auto-set monitoring target to: {clean_recipient}")
    
    # Record when sending to this recipient starts (for accurate reply counting)
    current_time = time.time()
    reply_state['sending_start_times'][clean_recipient] = current_time
    
    # Clear previous reply and duplicate data for this recipient to start fresh
    if clean_recipient in reply_state['replies_received']:
        reply_state['replies_received'][clean_recipient].clear()
    if clean_recipient in reply_state['duplicate_replies']:
        reply_state['duplicate_replies'][clean_recipient].clear()
    if clean_recipient in reply_state['found_matches']:
        reply_state['found_matches'][clean_recipient].clear()
    if clean_recipient in reply_state['lifetime_duplicate_count']:
        reply_state['lifetime_duplicate_count'][clean_recipient].clear()
    if clean_recipient in reply_state['number_timestamps']:
        reply_state['number_timestamps'][clean_recipient].clear()
    
    print(f"Started sending session to {clean_recipient} at {current_time} - reset counters for accurate tracking")
    sending_state['last_message_sent'] = ''
    sending_state['sending_speed'] = 0
    sending_state['current_number'] = '--'
    sending_state['estimated_time_remaining'] = 0

    # Start background sending and return immediately to show stop button
    def background_send():
        
        async def _upload():
            # Check if API credentials are available
            if not API_ID or not API_HASH:
                return 'error:Telegram API credentials not configured. Please contact administrator.'
            
            # Use telegram_lock to prevent concurrent client access to same session
            with telegram_lock:
                # Use the StringSession that was created during authentication
                monitoring_session_string = auth_state.get('monitoring_session_string')
                if not monitoring_session_string:
                    return 'error:Session not available. Please login again.'
                
                client = TelegramClient(StringSession(monitoring_session_string), API_ID, API_HASH)
                try:
                    await client.connect()
                    
                    # Quick authorization check - use existing session directly with retry
                    try:
                        if not await client.is_user_authorized():
                            # Try to reconnect once before failing
                            client.disconnect()
                            await asyncio.sleep(1)
                            await client.connect()
                            if not await client.is_user_authorized():
                                auth_state['is_authenticated'] = False
                                return 'error:Session expired. Please login again.'
                    except Exception as e:
                        print(f"Authorization check failed: {e}")
                        # If we can't check authorization, don't fail immediately
                        # Let it proceed and fail on actual operations if needed
                
                    # Resolve recipient (user or bot) with multiple attempts
                    entity = None
                    clean_recipient = recipient.lstrip('@')  # Initialize here
                    try:
                        # First try: exact username
                        entity = await client.get_entity(recipient)
                    except Exception as e1:
                        print(f"First attempt failed for {recipient}: {e1}")
                        try:
                            # Second try: without @ symbol if it exists
                            entity = await client.get_entity(clean_recipient)
                        except Exception as e2:
                            print(f"Second attempt failed for {clean_recipient}: {e2}")
                            try:
                                # Third try: with @ symbol if it doesn't exist
                                if not recipient.startswith('@'):
                                    at_recipient = '@' + recipient
                                    entity = await client.get_entity(at_recipient)
                                else:
                                    raise e2
                            except Exception as e3:
                                print(f"All attempts failed for {recipient}")
                                # If it's specifically wsotp200bot, provide a helpful message
                                if 'wsotp200bot' in recipient.lower():
                                    return f'error:The bot "@wsotp200bot" appears to be unavailable or doesn\'t exist. Please verify the bot is active or try a different recipient. You can test with your own username first.'
                                else:
                                    return f'error:Could not find recipient "{recipient}". Please check the username and ensure it exists on Telegram.'
                
                    # Handle XLSX, CSV or TXT file
                    if file_payload:
                        filename = file_payload['filename']
                        file_data = file_payload['data']
                        
                        if not (filename.endswith('.xlsx') or filename.endswith('.csv') or filename.endswith('.txt')):
                            return 'error:File must be a XLSX, CSV or TXT file.'
                        try:
                            if filename.endswith('.xlsx'):
                                # Handle XLSX file - use BytesIO to wrap the bytes
                                from io import BytesIO
                                file_stream = BytesIO(file_data)
                                df = pd.read_excel(file_stream, engine='openpyxl')
                                # Clean up data for better messaging
                                # Remove auto-generated index columns and empty columns
                                df = df.loc[:, ~df.columns.str.startswith('Unnamed:')]
                                df = df.dropna(axis=1, how='all')  # Remove completely empty columns
                            elif filename.endswith('.csv'):
                                # Handle CSV file - use BytesIO to wrap the bytes
                                from io import BytesIO
                                file_stream = BytesIO(file_data)
                                df = pd.read_csv(file_stream)
                                # Clean up data for better messaging
                                # Remove auto-generated index columns and empty columns
                                df = df.loc[:, ~df.columns.str.startswith('Unnamed:')]
                                df = df.dropna(axis=1, how='all')  # Remove completely empty columns
                            else:
                                # Handle TXT file - decode the bytes to string
                                file_content = file_data.decode('utf-8')
                                lines = [line.strip() for line in file_content.splitlines() if line.strip()]
                                if not lines:
                                    return 'error:TXT file is empty or contains no valid data.'
                                # Create a DataFrame with a single column containing all lines
                                df = pd.DataFrame({'data': lines})
                        except Exception as e:
                            print(f"File processing error: {str(e)}")
                            if filename.endswith('.xlsx'):
                                file_type = 'XLSX'
                            elif filename.endswith('.csv'):
                                file_type = 'CSV'
                            else:
                                file_type = 'TXT'
                            return f'error:Invalid {file_type} file. Error: {str(e)}'
                    else:
                        # Handle manual input (treat as single-column CSV)
                        if not manual_data:
                            return 'error:Manual data cannot be empty.'
                        
                        # Parse manual input - split by newlines only
                        if '\n' in manual_data:
                            # Split by newlines if present
                            data = [x.strip() for x in manual_data.split('\n') if x.strip()]
                        else:
                            # If no newlines, treat entire input as one message
                            data = [manual_data.strip()]
                        
                        df = pd.DataFrame({'Manual Input': data})
                    
                    # Send data based on selected mode
                    if send_mode == 'rows':
                        # Row by row: Send each row combining all columns
                        # Send with 1-second delay between messages
                        await send_row_data(client, entity, df, delay=1)
                        return 'success:Data sent successfully! All rows processed.'
                    else:
                        # Column by column: Send each column's data separately (default)
                        for col in df.columns:
                            # Send with 1-second delay between messages
                            await send_column_data(client, entity, df[col], col, delay=1)
                        return 'success:Data sent successfully! All columns processed.'
                finally:
                    try:
                        if client is not None and hasattr(client, 'disconnect'):
                            await asyncio.sleep(0.5)  # Brief delay before disconnect
                            client.disconnect()
                    except Exception as e:
                        print(f"Disconnect error (ignored): {e}")
                        pass  # Ignore disconnect errors
        
        # Keep monitoring running continuously - no need to stop for sending
        
        try:
            result = asyncio.run(_upload())
            print(f"Sending completed with result: {result}")
            if isinstance(result, str) and result.startswith('error:'):
                sending_state['error_message'] = result[6:]
        except Exception as e:
            print(f"Background sending error: {str(e)}")
            sending_state['error_message'] = str(e)
        finally:
            # Always reset sending state when done
            sending_state['is_sending'] = False
            sending_state['should_stop'] = False
    
    # Start the background thread
    thread = threading.Thread(target=background_send)
    thread.daemon = True
    thread.start()
    
    # Check if this is an AJAX request
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or 'application/json' in request.headers.get('Accept', ''):
        return jsonify({'status': 'success', 'message': 'Message sending started! You can stop it anytime using the stop button.'})
    
    flash('Message sending started! You can stop it anytime using the stop button.', 'success')
    return redirect(url_for('dashboard'))

# Helper: Send column data line by line (one complete item per message)
async def send_column_data(client, entity, data_series, col_name, delay=0):
    """Send each data item as one complete message, one by one"""
    try:
        # Removed header message - send data directly
        print(f"Starting to send {len(data_series)} items from column '{col_name}'")
        
        # Use global counter for 100-message pause functionality
        
        for i, value in enumerate(data_series, 1):
            # Check if stop signal was sent
            if sending_state['should_stop']:
                print("Stop signal received, halting column sending...")
                # Removed stop message - just stop sending
                return
            
            # Skip first N items if skip_count is set
            if i <= sending_state['skip_count']:
                continue
                
            # Update progress
            sending_state['current_message'] = i - sending_state['skip_count']
            sending_state['total_messages'] = len(data_series) - sending_state['skip_count']
                
            # Better numeric formatting - avoid .0 for integers (consistent with row mode)
            if pd.isna(value):
                continue  # Skip NaN values
            elif (isinstance(value, (float, int)) or hasattr(value, 'dtype')) and str(value).replace('.0', '').replace('-', '').isdigit():
                # Handle both Python float/int and numpy numeric types
                try:
                    if float(value) == int(float(value)):
                        value_str = str(int(float(value)))  # Convert 1.0 to 1
                    else:
                        value_str = str(value).strip()
                except (ValueError, OverflowError):
                    value_str = str(value).strip()
            else:
                value_str = str(value).strip()
            
            if value_str:  # Only send non-empty values
                # Update current number being sent
                sending_state['current_number'] = value_str
                try:
                    print(f"Sending message {i}: '{value_str}'")
                    await client.send_message(entity, value_str)  # Send complete value as one message
                    sending_state['messages_sent_successfully'] += 1
                    sending_state['last_message_sent'] = value_str
                    print(f"Message {i} sent successfully")
                    
                    # Check for 100-message pause using global counter
                    if sending_state['messages_sent_successfully'] % 100 == 0:
                        print(f"Pausing for 2 minutes after sending {sending_state['messages_sent_successfully']} messages...")
                        await pause_with_countdown(120)  # 2 minutes = 120 seconds
                    
                    if delay > 0:
                        await asyncio.sleep(delay)  # Only delay if specified
                except FloodWaitError as e:
                    # Handle rate limiting by waiting the required time
                    print(f"Rate limit hit, waiting {e.seconds + 1} seconds...")
                    await asyncio.sleep(e.seconds + 1)
                    try:
                        await client.send_message(entity, value_str)  # Retry after waiting
                        sending_state['messages_sent_successfully'] += 1
                        sending_state['last_message_sent'] = value_str
                        print(f"Message {i} sent after rate limit wait")
                        
                        # Check for 100-message pause after retry success using global counter
                        if sending_state['messages_sent_successfully'] % 100 == 0:
                            print(f"Pausing for 2 minutes after sending {sending_state['messages_sent_successfully']} messages...")
                            await pause_with_countdown(120)  # 2 minutes = 120 seconds
                    except Exception as retry_e:
                        sending_state['messages_failed'] += 1
                        print(f"Failed to send item {i} '{value_str}' after rate limit wait: {retry_e}")
                except Exception as e:
                    # Log and continue with next item if individual message fails
                    sending_state['messages_failed'] += 1
                    print(f"Failed to send item {i} '{value_str}': {e}")
            else:
                print(f"Skipping empty/NaN value at position {i}")
        
        # Removed footer message - data sent directly
        print(f"Finished sending all items from column '{col_name}'")
    except Exception as e:
        print(f"Failed to send column '{col_name}': {e}")
        raise

# Helper: Send data row by row (one message per row combining all columns)
async def send_row_data(client, entity, df, delay=0):
    """Send each row as one message combining all columns"""
    try:
        total_rows = len(df)
        # Removed header message - send data directly
        print(f"Starting to send {total_rows} rows")
        
        # Use global counter for 100-message pause functionality
        
        for i, (index, row) in enumerate(df.iterrows(), 1):
            # Check if stop signal was sent
            if sending_state['should_stop']:
                print("Stop signal received, halting row sending...")
                # Removed stop message - just stop sending
                return
                
            # Update progress
            sending_state['current_message'] = i
            sending_state['total_messages'] = total_rows
                
            # Build message combining all non-empty columns for this row
            row_parts = []
            for col_name, value in row.items():
                # Better numeric formatting - avoid .0 for integers (handle numpy types)
                if pd.isna(value):
                    continue  # Skip NaN values
                elif (isinstance(value, (float, int)) or hasattr(value, 'dtype')) and str(value).replace('.0', '').replace('-', '').isdigit():
                    # Handle both Python float/int and numpy numeric types
                    try:
                        if float(value) == int(float(value)):
                            value_str = str(int(float(value)))  # Convert 1.0 to 1
                        else:
                            value_str = str(value).strip()
                    except (ValueError, OverflowError):
                        value_str = str(value).strip()
                else:
                    value_str = str(value).strip()
                
                if value_str:  # Only include non-empty values
                    if len(df.columns) == 1:
                        # For single column, just send the value
                        row_parts.append(value_str)
                    else:
                        # For multiple columns, include column name
                        row_parts.append(f"{col_name}: {value_str}")
            
            if row_parts:  # Only send if there's data
                message = " | ".join(row_parts)
                # Update current number being sent
                sending_state['current_number'] = message
                try:
                    print(f"Sending row {i}/{total_rows}: '{message}'")
                    await client.send_message(entity, message)
                    sending_state['messages_sent_successfully'] += 1
                    sending_state['last_message_sent'] = message
                    print(f"Row {i} sent successfully")
                    
                    # Check for 100-message pause using global counter
                    if sending_state['messages_sent_successfully'] % 100 == 0:
                        print(f"Pausing for 2 minutes after sending {sending_state['messages_sent_successfully']} messages...")
                        await pause_with_countdown(120)  # 2 minutes = 120 seconds
                    
                    if delay > 0:
                        await asyncio.sleep(delay)  # Only delay if specified
                except FloodWaitError as e:
                    # Handle rate limiting by waiting the required time
                    print(f"Rate limit hit, waiting {e.seconds + 1} seconds...")
                    await asyncio.sleep(e.seconds + 1)
                    try:
                        await client.send_message(entity, message)  # Retry after waiting
                        sending_state['messages_sent_successfully'] += 1
                        sending_state['last_message_sent'] = message
                        print(f"Row {i} sent after rate limit wait")
                        
                        # Check for 100-message pause after retry success using global counter
                        if sending_state['messages_sent_successfully'] % 100 == 0:
                            print(f"Pausing for 2 minutes after sending {sending_state['messages_sent_successfully']} messages...")
                            await pause_with_countdown(120)  # 2 minutes = 120 seconds
                    except Exception as retry_e:
                        sending_state['messages_failed'] += 1
                        print(f"Failed to send row {i} '{message}' after rate limit wait: {retry_e}")
                except Exception as e:
                    # Log and continue with next row if individual message fails
                    sending_state['messages_failed'] += 1
                    print(f"Failed to send row {i} '{message}': {e}")
            else:
                print(f"Skipping empty row {i}")
        
        # Removed footer message - data sent directly
        print(f"Finished sending all {total_rows} rows")
    except Exception as e:
        print(f"Failed to send rows: {e}")
        raise


# Helper function for pause with countdown
async def pause_with_countdown(duration_seconds=120):
    """Pause for specified duration while updating countdown every few seconds"""
    sending_state['is_paused'] = True
    sending_state['pause_countdown'] = duration_seconds
    
    print(f"Starting {duration_seconds}-second pause with countdown...")
    
    # Update countdown every 5 seconds
    update_interval = 5
    while sending_state['pause_countdown'] > 0 and not sending_state['should_stop']:
        sleep_time = min(update_interval, sending_state['pause_countdown'])
        await asyncio.sleep(sleep_time)
        sending_state['pause_countdown'] -= sleep_time
        
        if sending_state['pause_countdown'] > 0:
            minutes = sending_state['pause_countdown'] // 60
            seconds = sending_state['pause_countdown'] % 60
            print(f"Pause countdown: {minutes}m {seconds}s remaining...")
    
    # Clear pause state
    sending_state['is_paused'] = False
    sending_state['pause_countdown'] = 0
    print("Pause completed, resuming message sending...")

# Reply monitoring functions
def extract_number_pattern(number_str):
    """Extract last 4 digits from a number string for pattern matching"""
    # Remove all non-digit characters
    digits = ''.join(filter(str.isdigit, str(number_str)))
    if len(digits) >= 4:
        return digits[-4:]  # Only use last 4 digits for matching
    else:
        # For numbers with less than 4 digits, pad with zeros on the left
        return digits.zfill(4)  # Pad left with zeros to make 4 digits


def extract_otp_from_text(text):
    """Extract 6-digit OTP from text, removing hyphens and formatting.
    
    Handles multiple OTP bot formats:
    - Inline button format: "921-902"
    - Mazen bot format: "🔐 OTP: 955555" or "code 270-576"
    - Plain text: "123456"
    """
    import re
    if not text:
        return None
    
    # Pattern 1: Look for "OTP:" or "code" followed by number (Mazen bot format)
    # Matches: "OTP: 955555", "OTP:955555", "code 270-576", "code: 123456"
    pattern_otp_label = r'(?:OTP|code)[:\s]+(\d{3})\s*-?\s*(\d{3})'
    match = re.search(pattern_otp_label, text, re.IGNORECASE)
    if match:
        otp = match.group(1) + match.group(2)
        print(f"DEBUG: Extracted OTP after label: {match.group(0)} → {otp}")
        return otp
    
    # Pattern 2: Look for 6-digit OTP with hyphen in middle (e.g., "774-365", "921-902")
    pattern_hyphen = r'(\d{3})\s*-\s*(\d{3})'
    match = re.search(pattern_hyphen, text)
    if match:
        otp = match.group(1) + match.group(2)
        print(f"DEBUG: Extracted OTP with hyphen: {match.group(0)} → {otp}")
        return otp
    
    # Pattern 3: Look for plain 6-digit number with word boundaries
    pattern_plain = r'\b(\d{6})\b'
    match = re.search(pattern_plain, text)
    if match:
        otp = match.group(1)
        print(f"DEBUG: Extracted 6-digit OTP: {otp}")
        return otp
    
    # Pattern 4: Look for 6-digit number after common delimiters (colon, space, equals)
    # This catches cases where word boundary doesn't work well
    pattern_after_delim = r'[:\s=](\d{6})(?:\s|$|[^\d])'
    match = re.search(pattern_after_delim, text)
    if match:
        otp = match.group(1)
        print(f"DEBUG: Extracted 6-digit OTP after delimiter: {otp}")
        return otp
    
    return None


def extract_otp_from_message(message_text):
    """Legacy function - Extract 6-digit OTP from message text"""
    otp = extract_otp_from_text(message_text)
    if not otp:
        print(f"DEBUG: No 6-digit OTP found in message text: {message_text}")
    return otp


async def extract_otp_from_message_with_buttons(message):
    """Extract 6-digit OTP from message - checks INLINE BUTTONS first, then message text.
    
    This handles the OTP BOT format where OTP codes appear in inline buttons like:
    [921-902] [Full-Message]
    """
    import re
    
    if not message:
        return None
    
    # PRIORITY 1: Check inline buttons first (OTP BOT format)
    # The OTP is typically in the first button (e.g., "921-902")
    if message.buttons:
        print(f"DEBUG: Message has inline buttons, checking them for OTP...")
        for row in message.buttons:
            for button in row:
                button_text = button.text if hasattr(button, 'text') else str(button)
                print(f"DEBUG: Checking button: '{button_text}'")
                
                # Skip non-OTP buttons (common bot UI buttons)
                skip_keywords = ['full', 'message', 'visit', 'channel', 'contact', 'dev', 
                                 'subscribe', 'join', 'share', 'help', 'support', 'website']
                if any(keyword in button_text.lower() for keyword in skip_keywords):
                    print(f"DEBUG: Skipping non-OTP button: '{button_text}'")
                    continue
                
                # Try to extract OTP from button text
                otp = extract_otp_from_text(button_text)
                if otp:
                    print(f"DEBUG: ✅ Found OTP '{otp}' in inline button: '{button_text}'")
                    return otp
    
    # PRIORITY 2: Check reply_markup if buttons not directly accessible
    if hasattr(message, 'reply_markup') and message.reply_markup:
        from telethon.tl.types import ReplyInlineMarkup, KeyboardButtonCallback, KeyboardButtonUrl
        
        if isinstance(message.reply_markup, ReplyInlineMarkup):
            print(f"DEBUG: Checking reply_markup rows for OTP...")
            for row in message.reply_markup.rows:
                for button in row.buttons:
                    button_text = button.text if hasattr(button, 'text') else ""
                    print(f"DEBUG: Checking markup button: '{button_text}'")
                    
                    # Skip non-OTP buttons (common bot UI buttons)
                    skip_keywords = ['full', 'message', 'visit', 'channel', 'contact', 'dev', 
                                     'subscribe', 'join', 'share', 'help', 'support', 'website']
                    if any(keyword in button_text.lower() for keyword in skip_keywords):
                        print(f"DEBUG: Skipping non-OTP markup button: '{button_text}'")
                        continue
                    
                    # Try to extract OTP from button text
                    otp = extract_otp_from_text(button_text)
                    if otp:
                        print(f"DEBUG: ✅ Found OTP '{otp}' in reply_markup button: '{button_text}'")
                        return otp
    
    # PRIORITY 3: Fall back to message text
    message_text = message.text if message and message.text else ""
    if message_text:
        print(f"DEBUG: Checking message text for OTP...")
        otp = extract_otp_from_text(message_text)
        if otp:
            print(f"DEBUG: ✅ Found OTP '{otp}' in message text")
            return otp
    
    print(f"DEBUG: No 6-digit OTP found in message (buttons or text)")
    return None


async def search_groups_for_numbers(client, target_pattern=None, limit_groups=20, messages_per_group=200, after_timestamp=None):
    """Optimized search through selected groups for numbers by last 4 digits pattern, optionally filtering to messages sent before duplicate detection"""
    try:
        print(f"Starting optimized group search (limit: {limit_groups} groups, {messages_per_group} messages each)...")
        
        # Clean up stale cached data (older than 2 hours)
        import time
        current_time = time.time()
        cache_ttl = 7200  # 2 hours
        
        numbers_to_remove = []
        for number, refs in reply_state['group_numbers'].items():
            # Filter out stale references
            fresh_refs = [ref for ref in refs if (current_time - ref.get('cached_at', 0)) < cache_ttl]
            if fresh_refs:
                reply_state['group_numbers'][number] = fresh_refs
            else:
                numbers_to_remove.append(number)
        
        # Remove numbers with no fresh references
        for number in numbers_to_remove:
            del reply_state['group_numbers'][number]
        
        if numbers_to_remove:
            print(f"Cleaned up {len(numbers_to_remove)} stale cached numbers")
        
        # Don't clear existing data aggressively - TTL cleanup above is sufficient
        # Only clear if explicitly requested refresh (architect recommendation)
        if not reply_state['group_numbers']:
            print("DEBUG: Starting with empty cache - will populate during search")
        
        total_numbers = 0
        groups_searched = 0
        
        # Get dialogs - filter by target groups if specified
        dialogs = []
        target_groups = reply_state.get('target_groups', [])
        
        if target_groups:
            print(f"Filtering to target groups: {target_groups}")
            # Search only in specified target groups
            async for dialog in client.iter_dialogs():
                if (dialog.is_group or dialog.is_channel) and dialog.entity.id in target_groups:
                    dialogs.append(dialog)
                    print(f"Found target group: {dialog.name} (ID: {dialog.entity.id})")
        else:
            print("No target groups specified - searching all available groups")
            # Search all available groups (original behavior)
            async for dialog in client.iter_dialogs():
                if dialog.is_group or dialog.is_channel:
                    dialogs.append(dialog)
        
        # Sort by most recent activity (last message date)
        import datetime
        from datetime import timezone
        # Use timezone-aware minimum datetime to avoid comparison issues
        min_datetime = datetime.datetime.min.replace(tzinfo=timezone.utc)
        dialogs.sort(key=lambda d: d.date if d.date else min_datetime, reverse=True)
        
        # If target groups are specified, search all of them (no limit_groups restriction)
        # If no target groups, use the original limit_groups behavior
        groups_to_search = dialogs if target_groups else dialogs[:limit_groups]
        
        for dialog in groups_to_search:
            print(f"Searching in group: {dialog.name}")
            try:
                # Get entity info for compact storage
                peer_id = dialog.entity.id
                access_hash = getattr(dialog.entity, 'access_hash', None)
                
                # Search through recent messages in this group (fast scan)
                messages_found = 0
                async for message in client.iter_messages(dialog, limit=messages_per_group):
                    if message.text and messages_found < messages_per_group:
                        # Extract numbers from message text - improved to handle formatted numbers
                        import re
                        # First find potential number patterns (including formatted ones)
                        potential_numbers = re.findall(r'[\d\s\-\(\)\+\.]{4,}', message.text)
                        numbers = []
                        for candidate in potential_numbers:
                            # Normalize by removing all non-digits
                            digits_only = re.sub(r'\D', '', candidate)
                            if len(digits_only) >= 4:  # Consider numbers with 4+ digits for last 4 digit pattern matching
                                numbers.append(digits_only)
                        
                        for number in numbers:
                                pattern = extract_number_pattern(number)
                                
                                # Skip debug logging for speed
                                pass
                                
                                # If we're looking for a specific pattern, prioritize matches
                                if target_pattern and pattern != target_pattern:
                                    continue
                                
                                # Store compact message reference with TTL
                                import time
                                message_ref = {
                                    'peer_id': peer_id,
                                    'access_hash': access_hash,
                                    'msg_id': message.id,
                                    'pattern': pattern,
                                    'group_name': dialog.name,
                                    'number': number,
                                    'timestamp': message.date.timestamp() if message.date else 0,
                                    'cached_at': time.time()  # When this was cached for TTL management
                                }
                                
                                # Support multiple messages with same pattern but limit duplicates
                                # Use pattern as key for consistent lookup (architect recommendation)
                                if pattern not in reply_state['group_numbers']:
                                    reply_state['group_numbers'][pattern] = []
                                
                                # Only store if not already stored or if it's more recent
                                existing_refs = reply_state['group_numbers'][pattern]
                                if len(existing_refs) < 3:  # Limit to 3 references per pattern
                                    reply_state['group_numbers'][pattern].append(message_ref)
                                    total_numbers += 1
                                else:
                                    # Replace oldest reference with newer one
                                    oldest_ref = min(existing_refs, key=lambda r: r.get('timestamp', 0))
                                    if message_ref['timestamp'] > oldest_ref.get('timestamp', 0):
                                        existing_refs.remove(oldest_ref)
                                        existing_refs.append(message_ref)
                                
                                messages_found += 1
                                
                                # If we found the specific pattern we're looking for, we can stop searching this group
                                if target_pattern and pattern == target_pattern:
                                    print(f"Found target pattern {target_pattern} in group {dialog.name}")
                                    break
                                    
            except Exception as e:
                print(f"Error searching group {dialog.name}: {e}")
                continue
            
            groups_searched += 1
            
            # If we're looking for a specific pattern and found it, we can stop
            if target_pattern and any(
                any(ref.get('pattern') == target_pattern for ref in refs) 
                for refs in reply_state['group_numbers'].values()
            ):
                print(f"Found target pattern {target_pattern}, stopping search")
                break
        
        print(f"Optimized search complete: Found {total_numbers} number instances across {len(reply_state['group_numbers'])} unique patterns in {groups_searched} groups")
        
        # Debug: Show sample of stored patterns for verification
        if len(reply_state['group_numbers']) > 0:
            sample_count = 0
            print("Sample stored patterns and numbers:")
            for pattern, refs in reply_state['group_numbers'].items():
                if sample_count < 3:  # Show first 3 for verification
                    number = refs[0].get('number', 'N/A')
                    group_name = refs[0].get('group_name', 'Unknown')
                    print(f"  Pattern: {pattern}, Number: {number}, Group: {group_name}")
                    sample_count += 1
                else:
                    break
        return True
    except Exception as e:
        print(f"Error searching groups: {e}")
        return False


async def find_best_matching_message(target_pattern, original_number, after_timestamp=None):
    """Find the best matching message by last 4 digits pattern, optionally filtering to messages sent at/after reply timestamp"""
    exact_matches = []
    pattern_matches = []
    
    # Get target groups for filtering
    target_groups = reply_state.get('target_groups', [])
    
    # Pattern-based cache lookup (optimized for speed)
    pass
    
    # Direct pattern match lookup (most efficient with new cache structure)
    if target_pattern in reply_state['group_numbers']:
        for ref in reply_state['group_numbers'][target_pattern]:
            # Filter by target groups if specified
            if target_groups:
                peer_id = ref.get('peer_id')
                if peer_id not in target_groups:
                    print(f"DEBUG: Skipping match from group {ref.get('group_name')} (ID: {peer_id}) - not in target groups")
                    continue
            
            # Filter by timestamp - only accept messages at or after reply time
            if after_timestamp is not None:
                msg_timestamp = ref.get('timestamp', 0)
                if msg_timestamp < after_timestamp:
                    print(f"DEBUG: Skipping match from {ref.get('group_name')} - message too old (msg: {msg_timestamp}, cutoff: {after_timestamp})")
                    continue
            
            # Accept match
            print(f"DEBUG: FOUND PATTERN MATCH! Pattern {target_pattern} (last 4 digits) found in cache from group {ref.get('group_name')}")
            pattern_matches.append({
                'number': ref.get('number', original_number),
                'peer_id': ref['peer_id'],
                'access_hash': ref['access_hash'],
                'msg_id': ref['msg_id'],
                'group_name': ref['group_name'],
                'confidence': 'pattern'
            })
    
    # Skip debug output for speed
    pass
    
    # Return best match - prefer exact matches
    if exact_matches:
        best_match = exact_matches[0]  # Return first exact match
        print(f"Found EXACT match: {best_match['number']} in {best_match['group_name']}")
        return best_match
    elif pattern_matches:
        best_match = pattern_matches[0]  # Return first pattern match
        print(f"Found PATTERN match: {best_match['number']} in {best_match['group_name']} (pattern: {target_pattern})")
        return best_match
    
    return None


async def find_matching_message(target_pattern):
    """Legacy function for backward compatibility"""
    return await find_best_matching_message(target_pattern, "", None)


async def find_matching_number(target_pattern):
    """Legacy function - find a number that matches the target pattern (for backward compatibility)"""
    # Get target groups for filtering
    target_groups = reply_state.get('target_groups', [])
    
    for number, message_refs in reply_state['group_numbers'].items():
        for ref in message_refs:
            if ref.get('pattern') == target_pattern:
                # Filter by target groups if specified
                if target_groups:
                    peer_id = ref.get('peer_id')
                    if peer_id not in target_groups:
                        continue
                
                print(f"Found matching number: {number} (pattern: {target_pattern})")
                return number
    return None


async def search_with_timeout(client, target_recipient, number, pattern, reply_timestamp, target_dialog, reply_to_message):
    """
    Continuously search for a match for up to 4 minutes.
    Returns True if match found and auto-reply sent, False if timeout.
    """
    import time
    import asyncio
    
    search_key = (target_recipient, pattern)
    start_time = time.time()
    timeout_seconds = 240  # 4 minutes
    poll_interval = 1  # Check every 1 second for faster detection
    
    # Initialize pending search state
    with reply_state_lock:
        reply_state['pending_searches'][search_key] = {
            'start_time': start_time,
            'reply_time': reply_timestamp,
            'status': 'searching',
            'pattern': pattern,
            'number': number
        }
    
    print(f"🔍 Starting 4-minute search for pattern {pattern} (number: {number}) from {target_recipient}")
    print(f"   Search will check messages sent at/after timestamp {reply_timestamp}")
    
    while True:
        elapsed = time.time() - start_time
        
        # Check if timeout reached
        if elapsed >= timeout_seconds:
            print(f"⏱️ Search timeout (4 minutes) for pattern {pattern} - no match found")
            with reply_state_lock:
                if search_key in reply_state['pending_searches']:
                    del reply_state['pending_searches'][search_key]
                    print(f"🧹 Cleaned up timed-out search for pattern {pattern}")
            return False
        
        # Search for match in cache (only messages after reply time)
        matching_info = await find_best_matching_message(pattern, number, reply_timestamp)
        
        if matching_info:
            print(f"✅ Found match for pattern {pattern}! Sending auto-reply...")
            
            try:
                # Get the original message to extract OTP
                original_message = await client.get_messages(matching_info['peer_id'], ids=matching_info['msg_id'])
                
                # Extract OTP from the original message (checks inline buttons first, then text)
                otp = await extract_otp_from_message_with_buttons(original_message)
                
                if otp:
                    # Send only the OTP as a reply
                    await client.send_message(
                        target_dialog, 
                        otp,
                        reply_to=reply_to_message.id
                    )
                    print(f"✅ Successfully sent OTP '{otp}' as reply to {number} from {matching_info['group_name']}")
                    
                    # Update tracking and REMOVE completed search
                    with reply_state_lock:
                        if target_recipient not in reply_state['found_matches']:
                            reply_state['found_matches'][target_recipient] = {}
                        reply_state['found_matches'][target_recipient][pattern] = matching_info['number']
                        
                        if target_recipient not in reply_state['last_auto_reply']:
                            reply_state['last_auto_reply'][target_recipient] = {}
                        reply_state['last_auto_reply'][target_recipient][number] = time.time()
                        
                        if search_key in reply_state['pending_searches']:
                            del reply_state['pending_searches'][search_key]
                            print(f"🧹 Cleaned up completed search for pattern {pattern}")
                    
                    return True
                else:
                    print(f"⏭️ Match found but no 6-digit OTP in message - skipping auto-reply")
                    with reply_state_lock:
                        if search_key in reply_state['pending_searches']:
                            del reply_state['pending_searches'][search_key]
                            print(f"🧹 Cleaned up no-OTP search for pattern {pattern}")
                    return False
                    
            except Exception as e:
                print(f"Error sending auto-reply: {e}")
                with reply_state_lock:
                    if search_key in reply_state['pending_searches']:
                        del reply_state['pending_searches'][search_key]
                        print(f"🧹 Cleaned up error search for pattern {pattern}")
                return False
        
        # No match yet - wait and try again
        remaining = timeout_seconds - elapsed
        print(f"⏳ No match yet for pattern {pattern}, will check again in {poll_interval}s ({int(remaining)}s remaining)")
        await asyncio.sleep(poll_interval)


async def setup_realtime_group_monitoring(client):
    """Setup real-time monitoring of selected groups for instant number caching - dynamically adapts to target_groups changes"""
    from telethon import events
    import re
    import time
    
    print("🔥 Setting up DYNAMIC REAL-TIME monitoring for groups...")
    print("   Monitoring will automatically adapt when target groups change during the session")
    
    async def handle_new_group_message(event):
        """Handle new messages from monitored groups in real-time - dynamically checks target_groups"""
        try:
            if not event.message.text:
                return
            
            message = event.message
            
            # Get group info first to check if we should process this group
            try:
                chat = await event.get_chat()
                group_name = getattr(chat, 'title', f'Group {event.chat_id}')
                group_id = chat.id
                access_hash = getattr(chat, 'access_hash', None)
            except:
                group_name = f'Group {event.chat_id}'
                group_id = event.chat_id
                access_hash = None
            
            # DYNAMIC CHECK: Check current target_groups setting
            current_target_groups = reply_state.get('target_groups', [])
            
            # Filter by target groups if specified (dynamic check on each message!)
            if current_target_groups and group_id not in current_target_groups:
                # Skip this message - not from a targeted group
                return
            
            # Extract numbers from the message
            potential_numbers = re.findall(r'[\d\s\-\(\)\+\.]{4,}', message.text)
            numbers = []
            for candidate in potential_numbers:
                digits_only = re.sub(r'\D', '', candidate)
                if len(digits_only) >= 4:
                    numbers.append(digits_only)
            
            if not numbers:
                return
            
            # Cache each number immediately (THREAD-SAFE)
            current_time = time.time()
            for number in numbers:
                pattern = extract_number_pattern(number)
                
                message_ref = {
                    'peer_id': group_id,
                    'access_hash': access_hash,
                    'msg_id': message.id,
                    'pattern': pattern,
                    'group_name': group_name,
                    'number': number,
                    'timestamp': message.date.timestamp() if message.date else current_time,
                    'cached_at': current_time
                }
                
                # Store in cache with thread safety
                with reply_state_lock:
                    if pattern not in reply_state['group_numbers']:
                        reply_state['group_numbers'][pattern] = []
                    
                    existing_refs = reply_state['group_numbers'][pattern]
                    if len(existing_refs) < 3:
                        reply_state['group_numbers'][pattern].append(message_ref)
                        print(f"✅ REAL-TIME CACHE: Pattern {pattern} from {group_name} (number: {number})")
                    else:
                        # Replace oldest with newest
                        oldest_ref = min(existing_refs, key=lambda r: r.get('timestamp', 0))
                        if message_ref['timestamp'] > oldest_ref.get('timestamp', 0):
                            existing_refs.remove(oldest_ref)
                            existing_refs.append(message_ref)
                            print(f"✅ REAL-TIME CACHE UPDATE: Pattern {pattern} from {group_name}")
        
        except Exception as e:
            print(f"Error handling real-time group message: {e}")
    
    # Monitor ALL group/channel messages - handler will filter dynamically based on current target_groups
    client.add_event_handler(
        handle_new_group_message,
        events.NewMessage(func=lambda e: e.is_group or e.is_channel)
    )
    print(f"✅ DYNAMIC REAL-TIME group monitoring active - will adapt to target_groups changes in real-time")


async def reply_monitor_loop(client, monitoring_duration=3600):
    """Main loop to monitor for replies and handle duplicate detection"""
    try:
        print("Starting reply monitoring...")
        reply_state['monitoring'] = True
        
        # Setup real-time group monitoring for instant caching
        await setup_realtime_group_monitoring(client)
        
        start_time = asyncio.get_event_loop().time()
        
        while reply_state['monitoring'] and not monitoring_stop_event.is_set() and (asyncio.get_event_loop().time() - start_time) < monitoring_duration:
            try:
                # Only monitor the specific target recipient if set
                if not reply_state['target_recipient']:
                    print("No target recipient set for monitoring. Skipping this cycle.")
                    await asyncio.sleep(5)  # Wait before next check
                    continue
                
                try:
                    # Get the specific target recipient entity with error handling
                    try:
                        target_entity = await client.get_entity(reply_state['target_recipient'])
                    except Exception as entity_error:
                        print(f"Error getting target entity {reply_state['target_recipient']}: {entity_error}")
                        await asyncio.sleep(10)  # Wait longer on entity errors
                        continue
                    
                    target_dialog = None
                    
                    # Find the dialog for this entity with timeout protection
                    dialog_timeout = 0
                    async for dialog in client.iter_dialogs():
                        if dialog.entity.id == target_entity.id:
                            target_dialog = dialog
                            break
                        dialog_timeout += 1
                        if dialog_timeout > 100:  # Prevent infinite loops
                            print("Dialog search timeout - too many dialogs to process")
                            break
                    
                    if not target_dialog:
                        print(f"Could not find dialog for target recipient: {reply_state['target_recipient']}")
                        await asyncio.sleep(5)
                        continue
                    
                    # Monitor only the target recipient's messages - get more recent messages for better accuracy
                    recent_messages = []
                    try:
                        message_count = 0
                        async for message in client.iter_messages(target_dialog, limit=100):
                            if message.text and not message.out:  # Incoming message from target
                                recent_messages.append(message)
                                message_count += 1
                            # Rate limiting protection - add small delays every 10 messages
                            if message_count % 10 == 0:
                                await asyncio.sleep(0.1)  # Small delay to prevent rate limiting
                    except Exception as msg_error:
                        print(f"Error fetching messages from {reply_state['target_recipient']}: {msg_error}")
                        if "Too Many Requests" in str(msg_error) or "FLOOD_WAIT" in str(msg_error):
                            print("Rate limit detected, waiting 30 seconds...")
                            await asyncio.sleep(30)
                        else:
                            await asyncio.sleep(5)
                        continue  # Skip this cycle on message fetch errors
                    
                    # Process current batch of messages to find duplicates
                    current_numbers = {}
                    target_recipient = reply_state['target_recipient']
                    
                    # Initialize recipient data if needed
                    if target_recipient not in reply_state['found_matches']:
                        reply_state['found_matches'][target_recipient] = {}
                    if target_recipient not in reply_state['replies_received']:
                        reply_state['replies_received'][target_recipient] = []
                    if target_recipient not in reply_state['duplicate_replies']:
                        reply_state['duplicate_replies'][target_recipient] = {}
                    if target_recipient not in reply_state['number_timestamps']:
                        reply_state['number_timestamps'][target_recipient] = {}
                    if target_recipient not in reply_state['last_auto_reply']:
                        reply_state['last_auto_reply'][target_recipient] = {}
                    if target_recipient not in reply_state['lifetime_duplicate_count']:
                        reply_state['lifetime_duplicate_count'][target_recipient] = {}
                    
                    import re
                    
                    # First pass: collect all numbers from recent messages from target recipient only
                    # Note: recent_messages already contains only incoming messages from target_dialog (target recipient)
                    for message in recent_messages:
                        message_key = (target_entity.id, message.id)
                        if message_key in reply_state['processed_messages']:
                            continue  # Skip already processed messages
                            
                        # Only count messages as replies if they were received AFTER sending started to this recipient
                        sending_start_time = reply_state['sending_start_times'].get(target_recipient)
                        message_timestamp = message.date.timestamp() if message.date else 0
                        
                        # Check if this message is a valid reply (received after sending started)
                        is_valid_reply = (sending_start_time is not None and 
                                        message_timestamp > sending_start_time)
                        
                        # Thread-safe check for double-counting and reply addition
                        with reply_state_lock:
                            # Prevent double-counting: check if this message was already counted as a reply
                            already_counted = any(
                                reply['message_id'] == message.id 
                                for reply in reply_state['replies_received'][target_recipient]
                            )
                            
                            if is_valid_reply and not already_counted:
                                reply_state['replies_received'][target_recipient].append({
                                    'message_id': message.id,
                                    'text': message.text,
                                    'date': message.date,
                                    'timestamp': message_timestamp
                                })
                                current_count = len(reply_state['replies_received'][target_recipient])
                                print(f"REPLY COUNTED: Total replies from {target_recipient}: {current_count} (message: '{message.text[:50]}...')")
                            elif already_counted:
                                print(f"DUPLICATE REPLY SKIPPED: Message {message.id} already counted (text: '{message.text[:50]}...')")
                        
                        # Handle invalid replies (outside the thread-safe block)
                        if not is_valid_reply:
                            # Log skipped message for debugging - be more explicit
                            if sending_start_time is None:
                                print(f"SKIPPED: Message from {target_recipient} - no sending start time set (message: '{message.text[:50]}...')")
                            else:
                                print(f"SKIPPED: Old message from {target_recipient} - received before sending session started (message timestamp: {message_timestamp}, session start: {sending_start_time})")
                            continue  # Skip this message completely if it's not a valid reply
                        
                        message_text = message.text.strip()
                        # Enhanced number extraction to handle more formatted numbers and edge cases
                        potential_numbers = re.findall(r'[\d\s\-\(\)\+\.]{4,}', message_text)
                        numbers = []
                        for candidate in potential_numbers:
                            # Normalize by removing all non-digits
                            digits_only = re.sub(r'\D', '', candidate)
                            # Accept numbers with 4+ digits (to match last 4 digits pattern matching)
                            if len(digits_only) >= 4:  # Match the pattern matching threshold
                                numbers.append(digits_only)
                        
                        # Also try to extract numbers from messages that might not match the regex pattern
                        # Look for sequences of at least 4 digits anywhere in the text
                        digit_sequences = re.findall(r'\d{4,}', message_text)
                        for seq in digit_sequences:
                            if seq not in numbers:  # Avoid duplicates
                                numbers.append(seq)
                        
                        # CRITICAL: Deduplicate numbers list to prevent counting same number multiple times from one message
                        # If a message says "Your code is 123456, enter 123456" we should only count it once
                        numbers = list(set(numbers))
                        
                        # Mark this message as processed immediately to avoid double counting
                        reply_state['processed_messages'].add(message_key)
                        
                        for number in numbers:
                                import time
                                current_timestamp = time.time()
                                window_count = 0
                                previous_count = 0
                                lifetime_count = 0
                                
                                # Thread-safe update of reply_state with proper enforcement
                                with reply_state_lock:
                                    # Initialize number timestamps if needed
                                    if number not in reply_state['number_timestamps'][target_recipient]:
                                        reply_state['number_timestamps'][target_recipient][number] = []
                                    
                                    # Initialize lifetime counter if needed
                                    if number not in reply_state['lifetime_duplicate_count'][target_recipient]:
                                        reply_state['lifetime_duplicate_count'][target_recipient][number] = 0
                                    
                                    # CRITICAL: Clean up old timestamps outside the time window (enforce TTL) BEFORE adding new one
                                    time_window = reply_state['duplicate_time_window']
                                    cutoff_time = current_timestamp - time_window
                                    reply_state['number_timestamps'][target_recipient][number] = [
                                        ts for ts in reply_state['number_timestamps'][target_recipient][number] 
                                        if ts >= cutoff_time
                                    ]
                                    
                                    # Count PREVIOUS occurrences (before adding current one) - this is for window-based duplicate detection
                                    previous_count = len(reply_state['number_timestamps'][target_recipient][number])
                                    
                                    # Get lifetime count (total duplicates ever seen from this recipient)
                                    lifetime_count = reply_state['lifetime_duplicate_count'][target_recipient][number]
                                    
                                    # If this is a duplicate (seen before), increment the lifetime counter
                                    if previous_count > 0:
                                        reply_state['lifetime_duplicate_count'][target_recipient][number] += 1
                                        lifetime_count = reply_state['lifetime_duplicate_count'][target_recipient][number]
                                    
                                    # Add current timestamp for this number
                                    reply_state['number_timestamps'][target_recipient][number].append(current_timestamp)
                                    
                                    # Total count including current message
                                    window_count = len(reply_state['number_timestamps'][target_recipient][number])
                                    
                                    # Update duplicate count based on ENFORCED time window
                                    reply_state['duplicate_replies'][target_recipient][number] = window_count
                                    
                                    if lifetime_count > 0:
                                        print(f"DUPLICATE TRACKING: Number {number} - Lifetime: {lifetime_count} duplicate(s), Window: {previous_count} before (total now: {window_count}) within {time_window//60}min from {target_recipient}")
                                    else:
                                        print(f"DUPLICATE TRACKING: Number {number} seen for first time from {target_recipient}")
                                
                                # Also track for current batch processing
                                if number not in current_numbers:
                                    current_numbers[number] = []
                                current_numbers[number].append({
                                    'message': message,
                                    'message_key': message_key,
                                    'timestamp': current_timestamp,
                                    'window_count': window_count,
                                    'previous_count': previous_count,
                                    'lifetime_count': lifetime_count
                                })
                                
                                # AUTO-REPLY SEARCH: For ANY reply with a number, start search (not just duplicates)
                                if previous_count == 0:  # This is the first time seeing this number in this session
                                    print(f"NEW NUMBER DETECTED: {number} - starting search for match")
                                    target_pattern = extract_number_pattern(number)
                                    search_key = (target_recipient, target_pattern)
                                    
                                    # Check if there's already an ACTIVE search for this pattern
                                    # Only skip if status is 'searching', not if it's completed/timeout/error
                                    with reply_state_lock:
                                        is_actively_searching = (
                                            search_key in reply_state['pending_searches'] and 
                                            reply_state['pending_searches'][search_key].get('status') == 'searching'
                                        )
                                        if is_actively_searching:
                                            print(f"⏭️ Active search already in progress for pattern {target_pattern}, skipping")
                                    
                                    if not is_actively_searching:
                                        # Check if immediate match exists in cache first
                                        matching_info = await find_best_matching_message(target_pattern, number, message_timestamp)
                                        
                                        if matching_info:
                                            # Immediate match found - send reply right away
                                            print(f"✅ IMMEDIATE MATCH: {matching_info['number']} in {matching_info['group_name']} - sending instant reply!")
                                            
                                            # Spam prevention check
                                            current_time = time.time()
                                            spam_prevention_window = 0.5
                                            should_skip_spam = False
                                            
                                            with reply_state_lock:
                                                last_reply_time = reply_state['last_auto_reply'][target_recipient].get(number, 0)
                                                if current_time - last_reply_time < spam_prevention_window:
                                                    should_skip_spam = True
                                            
                                            if not should_skip_spam:
                                                try:
                                                    # Get the original message to extract OTP (ONLY send 6-digit OTP)
                                                    try:
                                                        # Use peer_id directly - Telethon handles the entity resolution
                                                        original_message = await client.get_messages(matching_info['peer_id'], ids=matching_info['msg_id'])
                                                        # Extract OTP (checks inline buttons first, then text)
                                                        otp = await extract_otp_from_message_with_buttons(original_message)
                                                        
                                                        if otp:
                                                            await client.send_message(target_dialog, otp, reply_to=message.id)
                                                            print(f"✅ INSTANT AUTO-REPLY: Sent OTP '{otp}' immediately (no duplicate needed!)")
                                                            
                                                            # Update tracking only if OTP was sent
                                                            with reply_state_lock:
                                                                reply_state['found_matches'][target_recipient][target_pattern] = matching_info['number']
                                                                reply_state['last_auto_reply'][target_recipient][number] = time.time()
                                                                print(f"TRACKING UPDATE: Recorded instant auto-reply for {number}")
                                                        else:
                                                            print(f"⏭️ No 6-digit OTP found in message - skipping auto-reply")
                                                    except Exception as msg_e:
                                                        print(f"Error getting original message: {msg_e} - skipping auto-reply")
                                                    
                                                except Exception as e:
                                                    print(f"Error sending instant auto-reply: {e}")
                                            else:
                                                print(f"Spam prevention: skipping instant auto-reply for {number}")
                                        else:
                                            # No immediate match - start background search with 2-minute timeout
                                            print(f"⏳ No immediate match for pattern {target_pattern} - starting 2-minute background search")
                                            # Launch async search task that runs in background
                                            asyncio.create_task(
                                                search_with_timeout(
                                                    client,
                                                    target_recipient,
                                                    number,
                                                    target_pattern,
                                                    message_timestamp,
                                                    target_dialog,
                                                    message
                                                )
                                            )
                        
                        # Note: Messages will be marked as processed in the second pass after actual processing
                    
                    # Second pass: check for duplicates (both in current batch and within time window)
                    for number, message_infos in current_numbers.items():
                        # Check both current batch duplicates AND time window duplicates
                        current_batch_duplicates = len(message_infos) > 1
                        
                        # Use the LAST message's count for accuracy (it has the most recent count including all previous messages)
                        window_count = message_infos[-1]['window_count'] if message_infos else 0
                        previous_count = message_infos[-1]['previous_count'] if message_infos else 0
                        lifetime_count = message_infos[-1]['lifetime_count'] if message_infos else 0
                        time_window_duplicates = window_count >= 2
                        
                        # Calculate accurate lifetime duplicate count
                        # If current batch has duplicates, we need to account for them too
                        if current_batch_duplicates:
                            # Current batch adds (len - 1) duplicates (first is original, rest are duplicates)
                            # The lifetime_count from the last message already includes previous batch duplicates
                            # We just need to add the current batch duplicates (len - 1)
                            total_lifetime_duplicates = lifetime_count + (len(message_infos) - 1)
                            duplicate_type = "current batch"
                        else:
                            # No current batch duplicates, use the lifetime count as-is
                            total_lifetime_duplicates = lifetime_count
                            duplicate_type = "time window"
                        
                        if current_batch_duplicates or time_window_duplicates:
                            print(f"Found duplicate in {duplicate_type} from {target_recipient}: {number} ({total_lifetime_duplicates} total duplicate(s) from this recipient)")
                            
                            # Record duplicate detection timestamp for filtering
                            import time
                            duplicate_detection_time = time.time()
                            print(f"DUPLICATE DETECTED: Recording timestamp {duplicate_detection_time} for number {number}")
                            
                            # Extract pattern and search for match
                            target_pattern = extract_number_pattern(number)
                            
                            # Remove pattern blocking - allow multiple auto-replies for same pattern when new duplicates are detected
                            
                            # CRITICAL: Thread-safe spam prevention enforcement
                            import time
                            current_time = time.time()
                            spam_prevention_window = 0.5  # 0.5 seconds between auto-replies for same number (reduced for faster OTP delivery)
                            should_skip_spam = False
                            
                            with reply_state_lock:
                                last_reply_time = reply_state['last_auto_reply'][target_recipient].get(number, 0)
                                if current_time - last_reply_time < spam_prevention_window:
                                    secs_remaining = int(spam_prevention_window - (current_time - last_reply_time))
                                    print(f"SPAM PREVENTION ENFORCED: auto-reply for {number} was sent {secs_remaining}s ago, skipping")
                                    should_skip_spam = True
                            
                            if should_skip_spam:
                                # Mark these messages as processed even if we skip
                                for msg_info in message_infos:
                                    reply_state['processed_messages'].add(msg_info['message_key'])
                                continue
                            
                            # Find the best matching message from REAL-TIME cache by last 4 digits (instant lookup!)
                            print(f"Checking REAL-TIME cache for pattern: {target_pattern} (last 4 digits from number: {number})")
                            matching_info = await find_best_matching_message(target_pattern, number, duplicate_detection_time)
                            
                            if matching_info:
                                print(f"Found best matching message: {matching_info['number']} in {matching_info['group_name']} (confidence: {matching_info.get('confidence', 'exact')})")
                                try:
                                    # Find the most recent duplicate message to reply to
                                    reply_to_message = None
                                    for msg_info in message_infos:
                                        reply_to_message = msg_info['message']
                                        break  # Use the first (most recent) message
                                    
                                    if reply_to_message:
                                        # Get the original message to extract OTP (ONLY send 6-digit OTP)
                                        try:
                                            # Use peer_id directly - Telethon handles the entity resolution
                                            original_message = await client.get_messages(matching_info['peer_id'], ids=matching_info['msg_id'])
                                            
                                            # Extract OTP from the original message (checks inline buttons first, then text)
                                            otp = await extract_otp_from_message_with_buttons(original_message)
                                            
                                            if otp:
                                                # Send only the OTP as a reply to the duplicate
                                                await client.send_message(
                                                    target_dialog, 
                                                    otp,
                                                    reply_to=reply_to_message.id
                                                )
                                                print(f"✅ Successfully sent OTP '{otp}' as reply to duplicate {number}")
                                                
                                                # Update tracking (thread-safe, store numeric timestamp for spam prevention)
                                                with reply_state_lock:
                                                    reply_state['found_matches'][target_recipient][target_pattern] = matching_info['number']
                                                    reply_state['last_auto_reply'][target_recipient][number] = time.time()
                                                    print(f"TRACKING UPDATE: Recorded auto-reply for {number} to prevent spam")
                                            else:
                                                print(f"⏭️ No 6-digit OTP found in original message - skipping auto-reply")
                                        except Exception as msg_e:
                                            print(f"Error getting original message: {msg_e} - skipping auto-reply")
                                    else:
                                        print("No message found to reply to")
                                    
                                except Exception as e:
                                    print(f"Error sending auto-reply: {e} - skipping auto-reply")
                            else:
                                print(f"No matching message found for pattern {target_pattern} (from number {number})")
                                print(f"Available patterns in groups: {len(reply_state['group_numbers'])} numbers stored")
                                # Debug: show some available patterns
                                pattern_count = 0
                                for num, refs in reply_state['group_numbers'].items():
                                    if pattern_count < 5:  # Show first 5 for debugging
                                        for ref in refs[:1]:  # Show first ref for each number
                                            print(f"  Available: number={num}, pattern={ref.get('pattern', 'N/A')}")
                                            pattern_count += 1
                                    else:
                                        break
                            
                            # Mark all duplicate messages as processed
                            for msg_info in message_infos:
                                reply_state['processed_messages'].add(msg_info['message_key'])
                        
                        else:
                            # Single occurrence - just mark as processed
                            for msg_info in message_infos:
                                reply_state['processed_messages'].add(msg_info['message_key'])
                                
                except Exception as e:
                    print(f"Error getting target recipient {reply_state['target_recipient']}: {e}")
                    await asyncio.sleep(2)
                    continue
                
                # Quick check cycle for instant responses
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
                await asyncio.sleep(5)
                continue
    
    except Exception as e:
        print(f"Error in reply monitor: {e}")
    finally:
        # Don't set monitoring to False here - let the supervision loop handle restarts
        print("Reply monitor loop ended, will be restarted by supervision")


# Function to properly stop monitoring
def stop_monitoring_properly():
    """Properly stop monitoring by waiting for the thread to finish"""
    global monitoring_thread, monitoring_client
    
    if not reply_state['monitoring']:
        return True  # Already stopped
    
    print("Stopping monitoring for message sending...")
    
    # Set stop event and flag
    monitoring_stop_event.set()
    reply_state['monitoring'] = False
    
    # Wait for monitoring thread to finish (up to 15 seconds)
    if monitoring_thread and monitoring_thread.is_alive():
        monitoring_thread.join(timeout=15)
        if monitoring_thread.is_alive():
            print("Warning: Monitoring thread did not stop in time")
            return False
    
    # Additional wait for client to fully disconnect
    import time
    for i in range(5):
        if monitoring_client is None:
            break
        print(f"Waiting for monitoring client to disconnect... ({i+1}/5)")
        time.sleep(1)
    
    print("Monitoring stopped successfully")
    return True

# Function to auto-start monitoring (always active)
def auto_start_monitoring():
    """Automatically start reply monitoring when user logs in - always active, continuous monitoring"""
    global monitoring_thread
    
    if not auth_state['is_authenticated']:
        return False
    
    # Check if monitoring thread is already running to prevent duplicates
    if monitoring_thread and monitoring_thread.is_alive():
        print("Monitoring thread already running, skipping duplicate start")
        return True
    
    # Clear stop event when starting
    monitoring_stop_event.clear()
    
    def background_monitor():
        async def _monitor():
            global monitoring_client
            if not API_ID or not API_HASH:
                print("API credentials not available - monitoring disabled")
                return
            
            # Use StringSession for monitoring to avoid database conflicts
            monitoring_session_string = auth_state.get('monitoring_session_string')
            if not monitoring_session_string:
                print("No monitoring session string available - monitoring cannot start until login")
                return
            
            # ALWAYS ACTIVE: Continuous monitoring supervision with aggressive auto-restart
            backoff_delay = 1  # Start with 1 second backoff
            max_backoff = 60   # Maximum 1 minute backoff (reduced from 5 minutes)
            restart_count = 0
            
            print("Starting ALWAYS ACTIVE monitoring system...")
            
            # Infinite loop for always-active monitoring
            while auth_state['is_authenticated']:
                monitoring_client = TelegramClient(StringSession(monitoring_session_string), API_ID, API_HASH)
                try:
                    await monitoring_client.connect()
                    if await monitoring_client.is_user_authorized():
                        # Always set monitoring flag to true when running
                        reply_state['monitoring'] = True
                        restart_count = 0  # Reset restart count on successful connection
                        backoff_delay = 1  # Reset backoff delay
                        print(f"Monitoring active - continuous supervision started")
                        await reply_monitor_loop(monitoring_client)
                        print("Monitor loop ended, restarting immediately...")
                    else:
                        print("Monitoring session not authorized - will retry in 30 seconds")
                        await asyncio.sleep(30)
                        continue
                except FloodWaitError as e:
                    print(f"Rate limit hit in monitoring, waiting {e.seconds} seconds...")
                    reply_state['monitoring'] = True  # Still active, just waiting
                    await asyncio.sleep(e.seconds)
                    backoff_delay = 1  # Reset backoff on FloodWait
                except Exception as e:
                    restart_count += 1
                    print(f"Error in monitoring (restart #{restart_count}): {e}")
                    
                    # Apply exponential backoff for errors, but always restart
                    print(f"Monitoring will restart in {backoff_delay} seconds...")
                    await asyncio.sleep(backoff_delay)
                    backoff_delay = min(backoff_delay * 1.5, max_backoff)  # Slower exponential backoff
                finally:
                    # Always try to disconnect cleanly
                    try:
                        if monitoring_client is not None and hasattr(monitoring_client, 'disconnect'):
                            await asyncio.sleep(0.5)  # Brief delay before disconnect
                            disconnect_result = monitoring_client.disconnect()
                            if disconnect_result is not None:
                                await disconnect_result
                            monitoring_client = None
                    except Exception as e:
                        print(f"Monitoring disconnect error (ignored): {e}")
                        pass  # Ignore disconnect errors
                
                # Brief pause before restarting to avoid rapid cycling
                await asyncio.sleep(2)
                print("Restarting monitoring system...")
            
            # Only exit if user is no longer authenticated
            reply_state['monitoring'] = False
            print("Always-active monitoring ended (user logged out)")
        
        asyncio.run(_monitor())
    
    monitoring_thread = threading.Thread(target=background_monitor)
    monitoring_thread.daemon = True
    monitoring_thread.start()
    print("ALWAYS ACTIVE monitoring system started")
    return True

# Watchdog system to ensure monitoring stays active
monitoring_watchdog_thread = None
watchdog_stop_event = threading.Event()

def start_monitoring_watchdog():
    """Start a watchdog thread that ensures monitoring stays active"""
    global monitoring_watchdog_thread
    
    if monitoring_watchdog_thread and monitoring_watchdog_thread.is_alive():
        print("Watchdog already running, skipping duplicate start")
        return  # Watchdog already running
    
    watchdog_stop_event.clear()
    
    def watchdog_loop():
        """Watchdog that checks monitoring status every 30 seconds"""
        print("Starting monitoring watchdog...")
        
        while not watchdog_stop_event.is_set():
            try:
                # Wait for 30 seconds before checking
                if watchdog_stop_event.wait(30):
                    break  # Stop event was set
                
                # Only check if user is authenticated
                if auth_state['is_authenticated']:
                    # For authenticated users, monitoring should always appear active
                    reply_state['monitoring'] = True
                    
                    # Check if monitoring thread is still running
                    if not monitoring_thread or not monitoring_thread.is_alive():
                        print("Watchdog detected monitoring thread is down - attempting restart...")
                        
                        # Try to restart monitoring
                        if auto_start_monitoring():
                            print("Watchdog successfully restarted monitoring")
                        else:
                            print("Watchdog failed to restart monitoring - will try again in 30 seconds")
                    else:
                        # Monitoring thread is running
                        print("Watchdog check: Monitoring thread is active")
                else:
                    # User not authenticated, monitoring should be off
                    reply_state['monitoring'] = False
                    print("Watchdog: User not authenticated, monitoring correctly stopped")
                    
            except Exception as e:
                print(f"Watchdog error: {e}")
                # Continue running even if there's an error
                continue
        
        print("Monitoring watchdog stopped")
    
    monitoring_watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
    monitoring_watchdog_thread.start()
    print("Monitoring watchdog started")

def stop_monitoring_watchdog():
    """Stop the monitoring watchdog"""
    global monitoring_watchdog_thread
    
    if monitoring_watchdog_thread and monitoring_watchdog_thread.is_alive():
        print("Stopping monitoring watchdog...")
        watchdog_stop_event.set()
        monitoring_watchdog_thread.join(timeout=5)
        if monitoring_watchdog_thread.is_alive():
            print("Warning: Watchdog thread did not stop in time")
        else:
            print("Monitoring watchdog stopped successfully")

# Routes for reply monitoring
@app.route('/start_monitoring', methods=['POST'])
def start_monitoring():
    """Start reply monitoring"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    if reply_state['monitoring']:
        return jsonify({'status': 'error', 'message': 'Monitoring already running'})
    
    # Clear stop event when starting
    monitoring_stop_event.clear()
    
    def background_monitor():
        async def _monitor():
            if not API_ID or not API_HASH:
                print("No API credentials - monitoring appears active but cannot function")
                return
            
            # Set monitoring flag at start
            reply_state['monitoring'] = True
            
            session_name = auth_state.get('session_name', 'web_session')
            client = TelegramClient(session_name, API_ID, API_HASH)
            try:
                await client.connect()
                if await client.is_user_authorized():
                    await reply_monitor_loop(client)
                else:
                    print("Not authorized for monitoring")
            except Exception as e:
                print(f"Error in monitoring: {e}")
            finally:
                # Don't clear monitoring flag for always-active system
                try:
                    if client is not None and hasattr(client, 'disconnect'):
                        client.disconnect()
                except:
                    pass  # Ignore disconnect errors
        
        asyncio.run(_monitor())
    
    global monitoring_thread
    monitoring_thread = threading.Thread(target=background_monitor)
    monitoring_thread.daemon = True
    monitoring_thread.start()
    
    return jsonify({'status': 'success', 'message': 'Reply monitoring started'})


@app.route('/stop_monitoring', methods=['POST'])
def stop_monitoring():
    """Stop reply monitoring - DISABLED for always-active monitoring"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    # Always-active monitoring cannot be manually stopped
    return jsonify({'status': 'info', 'message': 'Monitoring is always active and cannot be manually stopped'})


@app.route('/set_target_recipient', methods=['POST'])
def set_target_recipient():
    """Set the target recipient for monitoring"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    recipient = request.form.get('recipient', '').strip()
    if not recipient:
        return jsonify({'status': 'error', 'message': 'Recipient is required'}), 400
    
    # Clean the recipient (remove @ if present)
    clean_recipient = recipient.lstrip('@')
    reply_state['target_recipient'] = clean_recipient
    
    # Clear previous monitoring data when changing target
    reply_state['replies_received'].clear()
    reply_state['duplicate_replies'].clear()
    reply_state['found_matches'].clear()
    reply_state['processed_messages'].clear()  # Also clear processed messages for fresh start
    
    print(f"Target recipient set to: {clean_recipient}")
    return jsonify({'status': 'success', 'message': f'Target recipient set to {clean_recipient}'})


@app.route('/get_available_groups', methods=['GET'])
def get_available_groups():
    """Get list of available groups/channels that the user has access to"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    if not API_ID or not API_HASH:
        return jsonify({'status': 'error', 'message': 'Telegram API credentials not configured'}), 400
    
    monitoring_session_string = auth_state.get('monitoring_session_string')
    if not monitoring_session_string:
        return jsonify({'status': 'error', 'message': 'No monitoring session available'}), 400
    
    try:
        import asyncio
        
        async def get_groups():
            # Type assertion since we already checked API_ID and API_HASH are not None
            api_id = API_ID
            api_hash = API_HASH
            if api_id is None or api_hash is None:
                raise ValueError("API credentials not available")
            
            client = TelegramClient(StringSession(monitoring_session_string), api_id, api_hash)
            try:
                await client.connect()
                groups = []
                
                async for dialog in client.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        groups.append({
                            'id': dialog.entity.id,
                            'name': dialog.name,
                            'type': 'channel' if dialog.is_channel else 'group',
                            'access_hash': getattr(dialog.entity, 'access_hash', None)
                        })
                
                return groups
            except Exception as e:
                print(f"Error getting groups: {e}")
                return []
            finally:
                try:
                    disconnect_result = client.disconnect()
                    if disconnect_result is not None:
                        await disconnect_result
                except:
                    pass
        
        # Run the async function with proper event loop management
        groups = asyncio.run(get_groups())
        
        return jsonify({
            'status': 'success', 
            'groups': groups,
            'current_target_groups': reply_state['target_groups']
        })
        
    except Exception as e:
        print(f"Error in get_available_groups: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to retrieve groups'}), 500


@app.route('/set_target_groups', methods=['POST'])
def set_target_groups():
    """Set the target groups for searching matching numbers"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    try:
        # Get the groups data from the request
        if request.is_json and request.json:
            group_ids = request.json.get('group_ids', [])
        else:
            # Handle form data (comma-separated string)
            group_ids_str = request.form.get('group_ids', '').strip()
            if group_ids_str:
                group_ids = [int(id.strip()) for id in group_ids_str.split(',') if id.strip()]
            else:
                group_ids = []
        
        # Validate that group_ids is a list
        if not isinstance(group_ids, list):
            return jsonify({'status': 'error', 'message': 'group_ids must be a list'}), 400
        
        # Convert to integers if they're strings
        try:
            normalized_group_ids = [int(gid) for gid in group_ids]
        except (ValueError, TypeError):
            return jsonify({'status': 'error', 'message': 'All group IDs must be valid integers'}), 400
        
        # Store the target groups in reply_state with thread safety
        with reply_state_lock:
            reply_state['target_groups'] = normalized_group_ids
            
            # Clear previous group search cache when changing target groups to force fresh search
            reply_state['group_numbers'].clear()
            reply_state['group_numbers_ttl'].clear()
            print(f"Target groups set to: {normalized_group_ids}")
            print("Cleared group search cache for fresh search with new target groups")
        
        group_count = len(normalized_group_ids)
        if group_count == 0:
            message = "Target groups cleared - will search all available groups"
        else:
            message = f"Target groups set to {group_count} group(s)"
        
        return jsonify({
            'status': 'success', 
            'message': message,
            'target_groups': normalized_group_ids
        })
        
    except Exception as e:
        print(f"Error in set_target_groups: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to set target groups'}), 500


@app.route('/monitoring_status', methods=['GET'])
def get_monitoring_status():
    """Get current monitoring status - always shows active for authenticated users"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    # For authenticated users, monitoring is always considered active
    # This ensures the UI never shows "Stopped" for logged-in users
    target_recipient = reply_state['target_recipient']
    
    # Count replies only from the target recipient
    target_replies_count = 0
    target_duplicates_count = 0
    target_matches_count = 0
    
    if target_recipient:
        # Count total replies from target recipient only - accurate counting from 1
        if target_recipient in reply_state['replies_received']:
            # Accurate count: 0 = no replies, 1 = first reply, 2 = second reply, etc.
            target_replies_count = len(reply_state['replies_received'][target_recipient])
        else:
            # No replies received yet
            target_replies_count = 0
        
        # Count duplicates from target recipient only (numbers that appear 2+ times)
        if target_recipient in reply_state['duplicate_replies']:
            target_duplicates_count = sum(1 for count in reply_state['duplicate_replies'][target_recipient].values() if count >= 2)
        
        # Count matches found for target recipient only
        if target_recipient in reply_state['found_matches']:
            target_matches_count = len(reply_state['found_matches'][target_recipient])
    
    # Check if monitoring thread is actually running and healthy
    monitoring_active = False
    if monitoring_thread and monitoring_thread.is_alive():
        monitoring_active = True
    
    return jsonify({
        'monitoring': monitoring_active,  # True only if thread is active
        'monitoring_always_active': True,  # UI flag for always-active design
        'target_recipient': target_recipient,  # Show who we're monitoring
        'total_replies': target_replies_count,  # Only from target recipient
        'duplicate_count': target_duplicates_count,  # Only from target recipient
        'matches_found': target_matches_count,  # Only for target recipient
        'group_numbers_count': len(reply_state['group_numbers']),
        'has_target_set': bool(target_recipient)  # Whether a target is configured
    })


@app.route('/reset_duplicates', methods=['POST'])
def reset_duplicates():
    """Reset duplicate counts to zero without restarting session"""
    if not auth_state['is_authenticated']:
        return jsonify({'status': 'error', 'message': 'Not authenticated'}), 401
    
    target_recipient = reply_state.get('target_recipient')
    if target_recipient:
        if target_recipient in reply_state['duplicate_replies']:
            reply_state['duplicate_replies'][target_recipient].clear()
        if target_recipient in reply_state['number_timestamps']:
            reply_state['number_timestamps'][target_recipient].clear()
        if target_recipient in reply_state['lifetime_duplicate_count']:
            reply_state['lifetime_duplicate_count'][target_recipient].clear()
        
        print(f"RESET: Cleared all duplicate tracking data for {target_recipient}")
        return jsonify({
            'status': 'success',
            'message': 'Duplicate counts reset to zero'
        })
    else:
        return jsonify({'status': 'error', 'message': 'No target recipient set'}), 400


@app.route('/health')
def health_check():
    """Simple health check endpoint"""
    return "OK", 200


if __name__ == '__main__':
    # Test extract_number_pattern function on startup
    test_cases = [
        ("12345678", "5678"),  # 8 digits: last 4 only
        ("123456789012", "9012"),  # 12 digits: last 4 only  
        ("1234", "1234"),  # 4 digits: use all 4
        ("12345", "2345"),  # 5 digits: last 4
        ("123", "0123"),  # 3 digits: pad left with zeros
        ("+1-234-567-8901", "8901"),  # With formatting: last 4
    ]
    print("Testing extract_number_pattern function:")
    for test_input, expected in test_cases:
        result = extract_number_pattern(test_input)
        status = "✅" if result == expected else "❌"
        print(f"  {status} '{test_input}' -> '{result}' (expected: '{expected}')")
    
    import os

    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)), debug=False)

