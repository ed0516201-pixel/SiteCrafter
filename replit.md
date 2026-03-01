# Overview

This is a Telegram File Sender web application built with Flask that allows users to authenticate with Telegram and send files through the Telegram API. The application provides a web interface for Telegram authentication and file transmission capabilities using the Telethon library for interacting with Telegram's API.

# Recent Changes

## November 1, 2025
- **Fixed Critical Memory Leak in Search System**: Resolved issue where completed searches (`pending_searches`) were never cleaned up, causing memory buildup and preventing new searches for previously-searched patterns. Now all searches (found/timeout/error/no_otp) are properly removed from memory after completion.
- **Fixed Skipped Matches Bug**: Corrected deduplication logic that was blocking new searches for patterns that had completed/timed-out searches. System now only skips if there's an ACTIVE search (status='searching'), allowing new searches after previous ones complete or timeout.
- **Prevented Search Accumulation**: Fixed issue where hundreds of concurrent 2-minute searches were running simultaneously when recipient sent many messages, causing severe delays and resource exhaustion. New cleanup system ensures completed searches are removed immediately.

## October 31, 2025
- **Implemented Search-on-Any-Reply System**: Completely redesigned the auto-reply trigger to search for matches on ANY reply from the recipient (not just duplicates). When a recipient sends ANY message with a number, the bot now starts a background search task that continuously looks for matches for up to 2 minutes.
- **Added 2-Minute Timeout Search with Continuous Polling**: Implemented `search_with_timeout` function that continuously polls the cache every 2 seconds for up to 2 minutes when no immediate match is found. This ensures OTPs that arrive slightly after the reply are still caught and auto-replied.
- **Added Time-Based Filtering**: Updated match finding to only consider messages sent at or after the recipient's reply time. This prevents matching against old/stale messages and ensures only fresh OTPs from groups are used for auto-replies.
- **Implemented Dynamic Group Monitoring**: Real-time group monitoring now dynamically checks `target_groups` on each incoming message, allowing instant adaptation when target groups are changed mid-session without requiring monitoring restart.
- **Fixed Target Group Filtering for Match Finding**: Updated `find_best_matching_message` and `find_matching_number` functions to properly filter matches by target groups. The bot now only returns matches from messages in the specified target groups, preventing false matches from other groups.
- **Implemented Real-Time Group Monitoring**: Completely redesigned the group search system to use Telegram's NewMessage event handlers for instant number caching. The system now monitors selected groups (or all groups if none specified) in real-time and caches numbers immediately as messages arrive, eliminating the need for historical message fetching.
- **Removed Historical Message Search Delays**: Eliminated all calls to `search_groups_for_numbers` that were causing delays by fetching up to 50 historical messages per group. The system now relies entirely on the real-time cache for instant lookups.
- **Achieved Sub-Second Reply Time**: Optimized the auto-reply system to respond within 0.5 seconds by checking the real-time cache instead of performing network-heavy historical searches. Numbers are cached as they arrive in groups, enabling instant pattern matching when recipients send duplicates.
- **Enhanced Thread Safety**: Added proper locking (`reply_state_lock`) around all cache mutations in the event handler to prevent data races and ensure cache consistency under concurrent access from multiple threads.
- **Fixed Default Monitoring Behavior**: Corrected the system to monitor all groups by default when no specific target groups are set, aligning runtime behavior with UI expectations.
- **OTP-Only Reply Mode**: Updated auto-reply system to send ONLY 6-digit OTPs extracted from matching group messages. If no 6-digit OTP is found in the original message, the system skips sending a reply entirely (no fallback to full numbers).

## October 11, 2025
- **Fixed Duplicate Count to Session-Only**: Fixed duplicate counting to only track duplicates from the current sending session. Previously, the `lifetime_duplicate_count` persisted across multiple sessions, causing incorrect duplicate counts. Now both `lifetime_duplicate_count` and `number_timestamps` are cleared when a new sending session starts for a recipient.
- **Fixed False Duplicate Detection**: Fixed issue where numbers appearing multiple times in the same message (e.g., "Your code is 123456, enter 123456") were incorrectly counted as duplicates. Now each unique number is counted only once per message, preventing false duplicate detection.

## October 10, 2025
- **Fixed Number Matching Threshold**: Updated both duplicate detection and group search to use consistent 4+ digit threshold (previously 7+ for duplicates, 6+ for groups). This ensures reliable matching based on last 4 digits for all number types including short OTPs and phone numbers.
- **Updated Search Time Window**: Changed search logic to look for messages sent AROUND THE SAME TIME as the duplicate (within 5 minutes before OR after), not just before. This allows finding OTPs that arrive at the same time or slightly after sending the number.
- **Added OTP Extraction Feature**: Implemented smart OTP extraction from matching messages. When a duplicate number is received from a recipient, the system:
  1. Extracts the last 4 digits as a pattern
  2. Searches target groups for messages with matching last 4 digits sent around the same time (±5 min)
  3. Finds the matching message in groups
  4. Extracts the 6-digit OTP from the message (handles formats like "774-365" → "774365")
  5. Sends only the OTP as a reply to the duplicate, tagging the recipient's message
  6. Falls back to sending the full number if no OTP is found

# User Preferences

Preferred communication style: Simple, everyday language.

# System Architecture

## Frontend Architecture
- **Web Interface**: Single-page Flask application using server-side rendered HTML templates
- **Styling**: Custom CSS with anime-inspired blue gradient background and animations
- **User Flow**: Multi-step authentication process (phone number → verification code → optional 2FA)
- **Session Management**: Flask sessions for maintaining authentication state across requests

## Backend Architecture
- **Web Framework**: Flask-based web server with route handlers for authentication and file operations
- **Async Integration**: Asyncio integration within Flask for handling Telegram's async API calls
- **Authentication Flow**: Multi-step Telegram authentication using phone number and verification codes
- **State Management**: In-memory authentication state tracking with session-based user data
- **File Processing**: Pandas integration for CSV file handling and processing
- **Auto-Reply System**: Real-time group monitoring with event-driven number caching, instant pattern matching (sub-0.5s response time), duplicate detection with 30-minute time window, message tagging, and OTP-only extraction (sends only 6-digit OTPs, skips if no OTP found)
- **Spam Prevention**: 0.5-second cooldown between auto-replies for the same number to prevent excessive messaging while enabling fast OTP delivery

## Security Model
- **Session Security**: Secure session key generation using secrets module
- **Environment-based Config**: API credentials stored in environment variables
- **No Persistent Sessions**: Authentication state maintained only during active sessions
- **Thread Safety**: Dedicated locks for concurrent access protection and state management

## API Integration Pattern
- **Telethon Client**: Telegram API client for authentication and message sending
- **Error Handling**: Comprehensive error handling for Telegram API errors (flood wait, invalid codes, etc.)
- **Session Management**: Dynamic Telegram session creation per user authentication
- **Performance Optimization**: Real-time event-driven caching with NewMessage handlers, TTL-based cache cleanup (2-hour expiry), and thread-safe cache mutations for instant pattern matching without network delays

# External Dependencies

## Core Framework Dependencies
- **Flask**: Web application framework for routing and templating
- **Pandas**: Data processing library for CSV file manipulation
- **Asyncio**: Python async/await support for Telegram API integration

## Telegram Integration
- **Telethon**: Primary Telegram API client library
- **Telegram API**: Requires API_ID and API_HASH from my.telegram.org
- **Authentication**: Phone-based authentication with optional 2FA support

## Required Environment Variables
- `TELEGRAM_API_ID`: Telegram API ID (integer, required)
- `TELEGRAM_API_HASH`: Telegram API hash (string, required)
- `SESSION_SECRET`: Flask session secret key (optional, auto-generated if not provided)

## Runtime Requirements
- Environment variables must be configured before application startup
- No external database dependencies (uses in-memory state)
- No external file storage (processes uploaded files in memory)