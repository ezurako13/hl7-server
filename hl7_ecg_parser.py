#!/usr/bin/env python3
"""
Simple HL7 ECG Parser
Optimized for HL7 files with known structure:
- ORU^W01 messages contain ECG waveforms
- 12 standard ECG leads (I, II, III, V1-V6, aVF, aVL, aVR)
- 100 samples per lead per message
- 100 Hz sampling rate (1 message per second)
"""

import numpy as np
import matplotlib.pyplot as plt
import os
import csv

class SimpleECGParser:
    def __init__(self, filepath):
        self.filepath = filepath
        self.ecg_data = {}
        self.metadata = {}
        
    def read_and_parse(self):
        """Read file and extract ECG data"""
        print(f"📖 Reading: {os.path.basename(self.filepath)}")
        
        with open(self.filepath, 'r', encoding='utf-8') as file:
            content = file.read()
        
        lines = content.strip().split('\n')
        
        # Extract ECG data organized by timestamp
        ecg_by_time = {}
        current_timestamp = None
        
        for line in lines:
            fields = line.split('|')
            segment_type = fields[0]
            
            # Track ECG waveform messages
            if segment_type == 'MSH' and len(fields) > 8:
                if fields[8] == 'ORU^W01':  # ECG waveform message
                    current_timestamp = fields[6]
            
            # Extract patient info from first PID
            elif segment_type == 'PID' and not self.metadata and len(fields) > 5:
                self.metadata = {
                    'patient_name': fields[5] if len(fields) > 5 else 'Unknown',
                    'patient_id': fields[3] if len(fields) > 3 else 'Unknown',
                    'birth_date': fields[7] if len(fields) > 7 else 'Unknown'
                }
            
            # Extract ECG waveforms
            elif segment_type == 'OBX' and current_timestamp and len(fields) > 5:
                observation_id = fields[3]
                observation_value = fields[5]
                
                # Only process ECG leads (exclude SpO2)
                if 'Ecg' in observation_id and 'SpO2' not in observation_id:
                    # Parse comma-separated values
                    if ',' in observation_value:
                        try:
                            data = [float(x.strip()) for x in observation_value.split(',')]
                            if len(data) == 100:  # Expected 100 samples
                                if current_timestamp not in ecg_by_time:
                                    ecg_by_time[current_timestamp] = {}
                                
                                # Clean lead name
                                lead_name = observation_id.split('^')[0].replace('Ecg ', '').replace(' Signal', '')
                                ecg_by_time[current_timestamp][lead_name] = np.array(data)
                        except:
                            continue
        
        # Create continuous waveforms
        if not ecg_by_time:
            print("❌ No ECG data found")
            return False
        
        # Sort timestamps and concatenate data
        sorted_timestamps = sorted(ecg_by_time.keys())
        all_leads = set()
        for timestamp_data in ecg_by_time.values():
            all_leads.update(timestamp_data.keys())
        
        # Create continuous signals
        continuous_data = {}
        for lead in sorted(all_leads):
            continuous_signal = []
            for timestamp in sorted_timestamps:
                if lead in ecg_by_time[timestamp]:
                    continuous_signal.extend(ecg_by_time[timestamp][lead])
            
            if continuous_signal:
                continuous_data[lead] = np.array(continuous_signal)
        
        self.ecg_data = continuous_data
        
        print(f"✅ Extracted {len(continuous_data)} ECG leads")
        print(f"📊 {len(sorted_timestamps)} time periods, {len(continuous_data[list(continuous_data.keys())[0]]):,} samples per lead")
        print(f"⏱️  Duration: {len(continuous_data[list(continuous_data.keys())[0]]) / 100:.0f} seconds at 100 Hz")
        
        return True
    
    def print_summary(self):
        """Print data summary"""
        print("\n" + "="*60)
        print("ECG DATA SUMMARY")
        print("="*60)
        
        if self.metadata:
            print("👤 Patient Information:")
            for key, value in self.metadata.items():
                print(f"   {key.replace('_', ' ').title()}: {value}")
        
        if self.ecg_data:
            print(f"\n💓 ECG Data (100 Hz sampling):")
            total_samples = len(list(self.ecg_data.values())[0])
            duration = total_samples / 100
            
            print(f"   Leads: {len(self.ecg_data)}")
            print(f"   Samples per lead: {total_samples:,}")
            print(f"   Recording duration: {duration:.0f} seconds ({duration/60:.1f} minutes)")
            
            print(f"\n📈 Lead Statistics:")
            for lead_name, waveform in self.ecg_data.items():
                mean_val = np.mean(waveform)
                std_val = np.std(waveform)
                range_val = np.max(waveform) - np.min(waveform)
                print(f"   {lead_name:>3}: Range {np.min(waveform):>6.2f} to {np.max(waveform):>6.2f} | "
                      f"Mean {mean_val:>6.3f} | Std {std_val:>5.3f}")
        
        print("="*60)
    
    def visualize(self, save_plot=True):
        """Create ECG visualization"""
        if not self.ecg_data:
            print("❌ No ECG data to visualize")
            return
        
        leads = list(self.ecg_data.keys())
        waveforms = list(self.ecg_data.values())
        
        # Create figure
        fig, axes = plt.subplots(len(leads), 1, figsize=(15, 2.5 * len(leads)))
        if len(leads) == 1:
            axes = [axes]
        
        plt.suptitle(f"ECG Data: {os.path.basename(self.filepath)} | 100 Hz Sampling", 
                    fontsize=16, fontweight='bold')
        
        # Plot each lead
        for i, (lead_name, waveform) in enumerate(zip(leads, waveforms)):
            time_axis = np.arange(len(waveform)) / 100  # 100 Hz sampling rate
            
            axes[i].plot(time_axis, waveform, 'b-', linewidth=0.8)
            axes[i].set_title(f"{lead_name} ({len(waveform):,} samples)", fontsize=12)
            axes[i].set_ylabel('Amplitude')
            axes[i].grid(True, alpha=0.3)
            
            # Add mean line
            axes[i].axhline(y=np.mean(waveform), color='r', linestyle='--', alpha=0.5)
            
            # Set reasonable y-limits
            y_range = np.max(waveform) - np.min(waveform)
            if y_range > 0:
                y_margin = y_range * 0.1
                axes[i].set_ylim(np.min(waveform) - y_margin, np.max(waveform) + y_margin)
        
        axes[-1].set_xlabel('Time (seconds)')
        plt.tight_layout()
        
        if save_plot:
            output_filename = f"{os.path.splitext(self.filepath)[0]}_ecg_simple.png"
            plt.savefig(output_filename, dpi=300, bbox_inches='tight')
            print(f"✅ Plot saved: {output_filename}")
        
        plt.show()
    
    def save_csv(self):
        """Save ECG data to CSV file"""
        if not self.ecg_data:
            print("❌ No ECG data to save")
            return
        
        output_filename = f"{os.path.splitext(self.filepath)[0]}_ecg_data.csv"
        
        with open(output_filename, 'w', newline='') as csvfile:
            # Create header
            leads = list(self.ecg_data.keys())
            header = ['Time_seconds'] + leads
            writer = csv.writer(csvfile)
            writer.writerow(header)
            
            # Write data
            max_length = max(len(waveform) for waveform in self.ecg_data.values())
            
            for i in range(max_length):
                time_val = i / 100  # 100 Hz sampling rate
                row = [f"{time_val:.3f}"]
                
                for lead in leads:
                    if i < len(self.ecg_data[lead]):
                        row.append(f"{self.ecg_data[lead][i]:.6f}")
                    else:
                        row.append('')
                
                writer.writerow(row)
        
        print(f"✅ CSV saved: {output_filename}")

def main():
    """Main function"""
    filename = "Ali 1.hl7"
    
    print("Simple HL7 ECG Parser")
    print("=" * 30)
    
    if not os.path.exists(filename):
        print(f"❌ File '{filename}' not found")
        return
    
    # Parse ECG data
    parser = SimpleECGParser(filename)
    
    if parser.read_and_parse():
        parser.print_summary()
        
        print(f"\n🎯 OPTIONS:")
        print("1. Visualize ECG data")
        print("2. Save to CSV file")
        print("3. Both visualize and save")
        
        choice = input("\nChoice (1-3) or Enter for 3: ").strip()
        
        if choice == '1':
            parser.visualize()
        elif choice == '2':
            parser.save_csv()
        else:  # Default to both
            parser.visualize()
            parser.save_csv()

if __name__ == "__main__":
    main()