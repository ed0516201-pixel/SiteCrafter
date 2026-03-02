        let sendingInterval;
        
        // Prevent form submission and handle via AJAX
        document.querySelector('form[action="/upload"]').addEventListener('submit', function(e) {
            e.preventDefault(); // Prevent page redirect
            
            const formData = new FormData(this);
            const recipient = formData.get('recipient');
            
            if (!recipient || recipient.trim() === '') {
                alert('Please enter a recipient.');
                return;
            }
            
            // Check if either file or manual data is provided
            const file = formData.get('file');
            const manualData = formData.get('manual_data');
            if ((!file || file.name === '') && (!manualData || manualData.trim() === '')) {
                alert('Please provide either a file (XLSX, CSV, or TXT) or manual data.');
                return;
            }
            
            // Disable the send button and show progress immediately
            document.getElementById('sendButton').disabled = true;
            document.getElementById('sendButton').textContent = 'Starting...';
            document.getElementById('sendingStatus').style.display = 'block';
            document.getElementById('statusText').textContent = '🚀 Starting message sending...';
            
            // Submit the form data via fetch
            fetch('/upload', {
                method: 'POST',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                },
                body: formData
            })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    return response.json().then(data => {
                        throw new Error(data.message || 'Failed to start sending');
                    }).catch(() => {
                        throw new Error('Server error - please try again');
                    });
                }
            })
            .then(data => {
                if (data.status === 'success') {
                    // Show success message
                    document.getElementById('statusText').textContent = '📤 Sending Messages...';
                    // Start polling for status immediately with 0.5 second real-time updates
                    if (!sendingInterval) {
                        sendingInterval = setInterval(checkSendingStatus, 500);
                    }
                } else {
                    throw new Error(data.message || 'Failed to start sending');
                }
            })
            .catch(error => {
                console.error('Error starting send:', error);
                // Show error to user
                const errorMsg = error.message || 'Failed to start sending. Please try again.';
                document.getElementById('statusText').textContent = '❌ Error: ' + errorMsg;
                document.getElementById('statusText').style.color = '#e74c3c';
                setTimeout(() => {
                    document.getElementById('statusText').style.color = '#4a90e2';
                    document.getElementById('statusText').textContent = '📤 Sending Messages...';
                }, 5000);
                
                // Reset UI on error
                document.getElementById('sendButton').disabled = false;
                document.getElementById('sendButton').textContent = 'Send to Recipient';
                document.getElementById('sendingStatus').style.display = 'none';
            });
        });
        
        function checkSendingStatus() {
            fetch('/status')
            .then(response => response.json())
            .then(data => {
                if (data.is_sending) {
                    document.getElementById('sendingStatus').style.display = 'block';
                    document.getElementById('progressInfo').textContent = `Progress: ${data.current_message}/${data.total_messages}`;
                    const percent = data.total_messages > 0 ? Math.round((data.current_message / data.total_messages) * 100) : 0;
                    document.getElementById('progressPercent').textContent = percent + '%';
                    document.getElementById('progressFill').style.width = percent + '%';
                    
                    document.getElementById('successCount').textContent = data.messages_sent_successfully;
                    document.getElementById('failedCount').textContent = data.messages_failed;
                    document.getElementById('totalMessagesSent').textContent = data.messages_sent_successfully;
                    document.getElementById('sendingSpeed').textContent = data.sending_speed.toFixed(1);
                    document.getElementById('currentRecipient').textContent = data.current_recipient;
                    document.getElementById('sendMode').textContent = data.send_mode;
                    document.getElementById('currentNumber').textContent = data.current_number;
                    document.getElementById('lastMessageSent').textContent = data.last_message_sent;
                    
                    if (data.estimated_time_remaining > 0) {
                        const mins = Math.floor(data.estimated_time_remaining / 60);
                        const secs = Math.floor(data.estimated_time_remaining % 60);
                        document.getElementById('timeRemaining').textContent = `${mins}:${secs.toString().padStart(2, '0')}`;
                    } else {
                        document.getElementById('timeRemaining').textContent = '--:--';
                    }
                    
                    if (data.is_paused) {
                        document.getElementById('statusText').textContent = `⏸️ Paused (${data.pause_countdown}s remaining)`;
                    } else {
                        document.getElementById('statusText').textContent = '📤 Sending Messages...';
                    }
                } else {
                    document.getElementById('sendButton').disabled = false;
                    document.getElementById('sendButton').textContent = 'Send to Recipient';
                    if (data.total_messages > 0 && data.current_message >= data.total_messages) {
                        document.getElementById('statusText').textContent = '✅ Sending Completed!';
                        clearInterval(sendingInterval);
                        sendingInterval = null;
                    }
                }
            });
        }
