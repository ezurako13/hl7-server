"""
Simple WFDB ECG Data Visualizer
Visualizes 12-lead ECG data from WFDB format files
"""

import wfdb
import matplotlib.pyplot as plt
import numpy as np
import os

def visualize_wfdb_ecg(record_path, save_plot=True):
    """
    Visualize 12-lead ECG from WFDB format
    
    Args:
        record_path: Path to WFDB record (without .hea extension)
        save_plot: Whether to save the plot as PNG
    """
    try:
        # Read WFDB record
        record = wfdb.rdrecord(record_path)
        
        # Extract ECG data and metadata
        ecg_data = record.p_signal  # Shape: (samples, leads)
        sampling_rate = record.fs
        lead_names = record.sig_name
        
        # Extract patient info and diagnosis
        patient_info = {}
        diagnosis = "Unknown"
        
        for comment in record.comments:
            if "Age:" in comment:
                patient_info['age'] = comment.split(": ")[1]
            elif "Sex:" in comment:
                patient_info['sex'] = comment.split(": ")[1]
            elif "Diagnosis:" in comment:
                diagnosis = comment.split(": ")[1]
        
        # Create time axis
        duration = len(ecg_data) / sampling_rate
        time_axis = np.linspace(0, duration, len(ecg_data))
        
        # Create figure with subplots for 12 leads
        fig, axes = plt.subplots(12, 1, figsize=(15, 20))
        
        # Plot title with patient info
        record_name = os.path.basename(record_path)
        title = f"12-Lead ECG: {record_name}"
        if patient_info:
            title += f" | Age: {patient_info.get('age', 'N/A')}, Sex: {patient_info.get('sex', 'N/A')}"
        title += f" | Diagnosis: {diagnosis}"
        fig.suptitle(title, fontsize=16, fontweight='bold')
        
        # Standard ECG lead order and colors
        lead_colors = ['blue', 'green', 'red', 'purple', 'orange', 'brown',
                      'pink', 'gray', 'olive', 'cyan', 'magenta', 'yellow']
        
        # Plot each lead
        for i, (lead_name, color) in enumerate(zip(lead_names, lead_colors)):
            axes[i].plot(time_axis, ecg_data[:, i], color=color, linewidth=0.8)
            axes[i].set_title(f"{lead_name} | Range: {np.min(ecg_data[:, i]):.2f} to {np.max(ecg_data[:, i]):.2f} mV", 
                             fontsize=10)
            axes[i].set_ylabel('Amplitude (mV)', fontsize=9)
            axes[i].grid(True, alpha=0.3)
            
            # Add mean line
            mean_val = np.mean(ecg_data[:, i])
            axes[i].axhline(y=mean_val, color='red', linestyle='--', alpha=0.5)
            
            # Set reasonable y-limits
            y_range = np.max(ecg_data[:, i]) - np.min(ecg_data[:, i])
            if y_range > 0:
                y_margin = y_range * 0.1
                axes[i].set_ylim(np.min(ecg_data[:, i]) - y_margin, 
                                np.max(ecg_data[:, i]) + y_margin)
        
        # Set x-label only for bottom plot
        axes[-1].set_xlabel('Time (seconds)', fontsize=12)
        
        # Adjust layout
        plt.tight_layout()
        
        # Save plot if requested
        if save_plot:
            output_filename = f"{record_path}_ecg_plot.png"
            plt.savefig(output_filename, dpi=300, bbox_inches='tight')
            print(f"✅ Plot saved: {output_filename}")
        
        plt.show()
        
        # Print summary
        print(f"\n📊 ECG DATA SUMMARY:")
        print(f"   Record: {record_name}")
        print(f"   Duration: {duration:.1f} seconds")
        print(f"   Sampling rate: {sampling_rate} Hz")
        print(f"   Total samples: {len(ecg_data):,}")
        print(f"   Number of leads: {len(lead_names)}")
        print(f"   Diagnosis: {diagnosis}")
        if patient_info:
            print(f"   Patient: Age {patient_info.get('age', 'N/A')}, {patient_info.get('sex', 'N/A')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error visualizing {record_path}: {e}")
        return False

def visualize_multiple_records(record_paths):
    """Visualize multiple WFDB records"""
    print(f"📈 Visualizing {len(record_paths)} WFDB records...")
    
    for i, record_path in enumerate(record_paths, 1):
        print(f"\n{i}. Processing: {os.path.basename(record_path)}")
        visualize_wfdb_ecg(record_path)
        
        if i < len(record_paths):
            input("Press Enter to continue to next record...")

def main():
    """Main function"""
    print("WFDB ECG Data Visualizer")
    print("=" * 30)
    
    # Example record paths (without .hea extension)
    sample_records = [
        "s0191lre_segment3",  # MI
        "signal_1560",       # NORM  
        "signal_3512"        # HYP
    ]
    
    # Check which records exist
    existing_records = []
    for record in sample_records:
        if os.path.exists(f"{record}.hea") and os.path.exists(f"{record}.dat"):
            existing_records.append(record)
        else:
            print(f"⚠️  Record {record} not found (need both .hea and .dat files)")
    
    if not existing_records:
        print("❌ No valid WFDB records found")
        print("Make sure you have .hea and .dat files in the current directory")
        return
    
    print(f"✅ Found {len(existing_records)} valid records")
    
    # Visualize option
    if len(existing_records) == 1:
        visualize_wfdb_ecg(existing_records[0])
    else:
        print(f"\nOptions:")
        print(f"1. Visualize all records one by one")
        for i, record in enumerate(existing_records, 2):
            diagnosis = "Unknown"
            try:
                header = wfdb.rdheader(record)
                for comment in header.comments:
                    if "Diagnosis:" in comment:
                        diagnosis = comment.split(": ")[1]
                        break
            except:
                pass
            print(f"{i}. Visualize {record} only ({diagnosis})")
        
        choice = input(f"\nChoice (1-{len(existing_records)+1}) or Enter for 1: ").strip()
        
        if choice == '' or choice == '1':
            visualize_multiple_records(existing_records)
        else:
            try:
                index = int(choice) - 2
                if 0 <= index < len(existing_records):
                    visualize_wfdb_ecg(existing_records[index])
                else:
                    print("Invalid choice")
            except:
                print("Invalid choice")

if __name__ == "__main__":
    main()
