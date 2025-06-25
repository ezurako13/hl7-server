#!/usr/bin/env python3
"""
Simple HL7 Splitter - Split long HL7 ECG file into 10-second chunks
"""

import os

def split_hl7_to_10_seconds(input_file):
    """Split HL7 file into 10-second chunks"""
    
    print(f"📖 Reading: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Group lines by complete messages (MSH to end of ECG data)
    messages = []
    current_message = []
    
    for line in lines:
        current_message.append(line)
        
        # End of ECG waveform message (after all OBX segments)
        if line.startswith('OBX') and 'SpO2' in line:
            messages.append(current_message)
            current_message = []
    
    # Add any remaining message
    if current_message:
        messages.append(current_message)
    
    print(f"📊 Found {len(messages)} ECG messages")
    
    # Create output directory
    output_dir = "hl7_10sec_chunks"
    os.makedirs(output_dir, exist_ok=True)
    print(f"📁 Output directory: {output_dir}")
    
    # Group into 10-second chunks (10 messages = 10 seconds)
    chunk_size = 10
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    
    for i in range(0, len(messages), chunk_size):
        chunk_messages = messages[i:i + chunk_size]
        
        if len(chunk_messages) < chunk_size and i + chunk_size < len(messages):
            continue  # Skip incomplete chunks unless it's the last one
        
        # Create output filename in output directory
        chunk_number = i // chunk_size + 1
        output_file = os.path.join(output_dir, f"{base_name}_10sec_chunk_{chunk_number:03d}.hl7")
        
        # Write chunk to file
        with open(output_file, 'w', encoding='utf-8') as f:
            for message in chunk_messages:
                f.writelines(message)
        
        duration = len(chunk_messages)
        print(f"✅ Created: {os.path.basename(output_file)} ({duration} seconds)")
    
    total_chunks = (len(messages) + chunk_size - 1) // chunk_size
    print(f"🎯 Split into {total_chunks} files in '{output_dir}' folder")

if __name__ == "__main__":
    input_file = "Ali 1.hl7"
    
    if not os.path.exists(input_file):
        print(f"❌ File {input_file} not found")
        exit(1)
    
    split_hl7_to_10_seconds(input_file)
    print("✅ Done!")