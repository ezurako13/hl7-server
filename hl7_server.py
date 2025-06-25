#!/usr/bin/env python3
"""
HL7 Server for receiving medical device messages
Designed for single-client production environment
"""

import socket
import threading
import logging
import datetime
import os
import hl7
from pathlib import Path

class HL7Server:
    def __init__(self, host='0.0.0.0', port=2575, message_dir='hl7_messages', max_files=1000):
        self.host = host
        self.port = port
        self.message_dir = Path(message_dir)
        self.max_files = max_files
        self.running = False
        self.server_socket = None
        self.file_lock = threading.Lock()  # Thread-safe file operations
        
        # Create message directory if it doesn't exist
        self.message_dir.mkdir(exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('hl7_server.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Log initial file count
        initial_count = self.count_message_files()
        self.logger.info(f"Message directory initialized with {initial_count} existing files (max: {self.max_files})")
        
        # Clean up if we're over the limit (batch cleanup)
        if initial_count > self.max_files:
            self.logger.info(f"Initial cleanup required: {initial_count} files exceed limit of {self.max_files}")
            self.cleanup_old_files()
    
    def count_message_files(self):
        """Count HL7 message files in directory"""
        try:
            return len([f for f in self.message_dir.glob('*.hl7')])
        except Exception:
            return 0
    
    def cleanup_old_files(self):
        """Remove approximately half of the files when limit is reached"""
        try:
            # Get all .hl7 files with their modification times
            files = []
            for file_path in self.message_dir.glob('*.hl7'):
                try:
                    mtime = file_path.stat().st_mtime
                    files.append((mtime, file_path))
                except Exception:
                    continue
            
            # Sort by modification time (oldest first)
            files.sort(key=lambda x: x[0])
            
            # Calculate how many files to remove (approximately half)
            files_to_remove = len(files) // 2
            
            if files_to_remove > 0:
                removed_count = 0
                for _, file_path in files[:files_to_remove]:
                    try:
                        file_path.unlink()
                        removed_count += 1
                    except Exception as e:
                        self.logger.warning(f"Could not remove old file {file_path.name}: {e}")
                
                if removed_count > 0:
                    remaining_files = len(files) - removed_count
                    self.logger.info(f"Batch cleanup: removed {removed_count} oldest files, {remaining_files} files remaining")
                    
        except Exception as e:
            self.logger.error(f"Error during file cleanup: {e}")
        
    def start(self):
        """Start the HL7 server"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Set socket timeout to make it responsive to interrupts
            self.server_socket.settimeout(1.0)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            
            self.running = True
            self.logger.info(f"HL7 Server started on {self.host}:{self.port}")
            self.logger.info(f"Messages will be saved to: {self.message_dir.absolute()}")
            self.logger.info("Press Ctrl+C to stop the server")
            
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    self.logger.info(f"Connection from {client_address}")
                    
                    # Handle each client in a separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except socket.timeout:
                    # Timeout is normal, just check if we should continue
                    continue
                except socket.error as e:
                    if self.running:
                        self.logger.error(f"Socket error: {e}")
                        
        except Exception as e:
            self.logger.error(f"Failed to start server: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the HL7 server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.logger.info("HL7 Server stopped")
    
    def handle_client(self, client_socket, client_address):
        """Handle individual client connection"""
        try:
            buffer = ""
            
            while self.running:
                # Receive data
                data = client_socket.recv(4096).decode('utf-8', errors='ignore')
                if not data:
                    break
                    
                buffer += data
                
                # Process complete HL7 messages
                while '\x1c' in buffer or '\r' in buffer:  # HL7 end markers
                    # Find message boundaries
                    end_pos = buffer.find('\x1c')  # File separator
                    if end_pos == -1:
                        end_pos = buffer.find('\r')  # Carriage return
                    
                    if end_pos == -1:
                        break
                        
                    # Extract complete message
                    message = buffer[:end_pos].strip()
                    buffer = buffer[end_pos + 1:]
                    
                    if message:
                        self.process_message(message, client_socket, client_address)
                        
        except Exception as e:
            self.logger.error(f"Error handling client {client_address}: {e}")
        finally:
            client_socket.close()
            self.logger.info(f"Connection closed for {client_address}")
    
    def process_message(self, message_text, client_socket, client_address):
        """Process received HL7 message"""
        try:
            # Parse HL7 message
            h = hl7.parse(message_text.replace('\n', '\r'))
            
            # Extract key information
            msh = h.segment('MSH')
            msg_control_id = str(msh[10]) if len(msh) > 10 else "UNKNOWN"
            msg_type = str(msh[9]) if len(msh) > 9 else "UNKNOWN"
            sending_app = str(msh[3]) if len(msh) > 3 else "UNKNOWN"
            
            self.logger.info(f"Received {msg_type} message (ID: {msg_control_id}) from {sending_app}")
            
            # Save message to file
            self.save_message(message_text, msg_control_id, msg_type, client_address)
            
            # Send ACK response
            ack_message = self.create_ack(msh)
            client_socket.send(ack_message.encode('utf-8'))
            
            self.logger.info(f"ACK sent for message {msg_control_id}")
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            # Send NAK (negative acknowledgment)
            try:
                nak_message = self.create_nak("UNKNOWN", str(e))
                client_socket.send(nak_message.encode('utf-8'))
            except:
                pass
    
    def save_message(self, message_text, control_id, msg_type, client_address):
        """Save HL7 message to file with batch cleanup when needed"""
        with self.file_lock:  # Thread-safe file operations
            try:
                # Check if we need to clean up old files first
                current_count = self.count_message_files()
                if current_count >= self.max_files:
                    self.logger.info(f"File limit reached ({current_count}/{self.max_files}), performing batch cleanup...")
                    self.cleanup_old_files()
                
                timestamp = datetime.datetime.now()
                
                # Create filename with timestamp and control ID
                filename = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{control_id}_{msg_type.replace('^', '_')}.hl7"
                filepath = self.message_dir / filename
                
                # Write message to file
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(f"# Received: {timestamp.isoformat()}\n")
                    f.write(f"# From: {client_address[0]}:{client_address[1]}\n")
                    f.write(f"# Control ID: {control_id}\n")
                    f.write(f"# Message Type: {msg_type}\n")
                    f.write("#" + "="*50 + "\n")
                    f.write(message_text)
                    f.write("\n")
                
                # Log save with current file count
                final_count = self.count_message_files()
                self.logger.info(f"Message saved to {filename} ({final_count}/{self.max_files} files)")
                
            except Exception as e:
                self.logger.error(f"Failed to save message: {e}")
    
    def create_ack(self, original_msh):
        """Create ACK (acknowledgment) message"""
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        control_id = str(original_msh[10]) if len(original_msh) > 10 else "UNKNOWN"
        
        ack = f"MSH|^~\\&|HL7_SERVER||{original_msh[3]}|{original_msh[4]}|{timestamp}||ACK|{control_id}_ACK|P|2.3.1\r"
        ack += f"MSA|AA|{control_id}|Message accepted\r"
        
        return ack + '\x1c'  # Add end-of-message marker
    
    def create_nak(self, control_id, error_msg):
        """Create NAK (negative acknowledgment) message"""
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        
        nak = f"MSH|^~\\&|HL7_SERVER||||{timestamp}||ACK|{control_id}_NAK|P|2.3.1\r"
        nak += f"MSA|AE|{control_id}|{error_msg[:100]}\r"  # Limit error message length
        
        return nak + '\x1c'

def main():
    """Main function to start the server"""
    import signal
    import sys
    
    # Configuration
    HOST = '0.0.0.0'  # Listen on all interfaces
    PORT = 2575       # Standard HL7 port
    MESSAGE_DIR = 'hl7_messages'
    MAX_FILES = 1000  # Maximum number of message files to keep
    
    # Create server
    server = HL7Server(HOST, PORT, MESSAGE_DIR, MAX_FILES)
    
    # Handle graceful shutdown
    def signal_handler(sig, frame):
        print("\n" + "="*50)
        print("🛑 SHUTDOWN SIGNAL RECEIVED")
        print("="*50)
        print("Stopping HL7 server...")
        server.stop()
        print("Server stopped successfully!")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal
    
    print("="*50)
    print("🏥 HL7 SERVER STARTING")
    print("="*50)
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print(f"Message Directory: {MESSAGE_DIR}")
    print(f"Max Files: {MAX_FILES} (auto-cleanup enabled)")
    print("\n📋 CONTROLS:")
    print("  • Press Ctrl+C to stop the server")
    print("  • Check 'hl7_server.log' for detailed logs")
    print("  • Old files auto-deleted when limit reached")
    print("="*50)
    
    try:
        server.start()
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received")
        server.stop()
        print("Server stopped!")

if __name__ == "__main__":
    main()