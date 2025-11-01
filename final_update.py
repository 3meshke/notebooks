import json

with open('04_drift_analysis_psi.ipynb', 'r', encoding='utf-8') as f:
    nb = json.load(f)

print(f"Total cells: {len(nb['cells'])}")

# UPDATE CELL 17: Monthly PSI Trends - Add plots  
cell_17 = nb['cells'][17]
source_17 = ''.join(cell_17['source']) if isinstance(cell_17['source'], list) else cell_17['source']

# Add PSI plots
if 'save_pandas_to_csv_adls(monthly_psi_df' in source_17 and 'table_trend_folder' not in source_17:
    print("Adding PSI trend plots to cell 17...")
    
    # Find the line with save_pandas_to_csv_adls
    lines = source_17.split('\n')
    new_lines = []
    
    for j, line in enumerate(lines):
        new_lines.append(line)
        
        if 'save_pandas_to_csv_adls(monthly_psi_df' in line and 'psi_monthly_trends' in line:
            # Add plots after CSV save
            new_lines.extend([
                '',
                '            # Create trend plots',
                '            table_trend_folder = f"{MONTHLY_TRENDS_PSI_PATH}{table_name}/"',
                '            dbutils.fs.mkdirs(table_trend_folder)',
                '',
                '            for feature in [f for f in num_features if f in df_spark.columns]:',
                '                try:',
                '                    feature_data = monthly_psi_df[monthly_psi_df[\'feature\'] == feature].sort_values(\'month\')',
                '                    if len(feature_data) > 0:',
                '                        fig, ax = plt.subplots(figsize=(12, 6))',
                '                        ax.plot(feature_data[\'month\'], feature_data[\'psi\'],',
                '                               marker=\'o\', linewidth=2, markersize=6, color=\'steelblue\')',
                '                        ax.axhline(y=PSI_THRESHOLD_MODERATE, color=\'orange\', linestyle=\'--\', linewidth=1.5, label=\'Moderate\')',
                '                        ax.axhline(y=PSI_THRESHOLD_SIGNIFICANT, color=\'red\', linestyle=\'--\', linewidth=1.5, label=\'Significant\')',
                '                        ax.set_title(f\'Monthly PSI: {feature}\\\\n({table_name})\', fontsize=12)',
                '                        ax.set_ylabel(\'PSI\', fontsize=10)',
                '                        ax.set_xlabel(\'Month\', fontsize=10)',
                '                        ax.legend(fontsize=9)',
                '                        ax.grid(True, alpha=0.3)',
                '                        ax.tick_params(axis=\'x\', rotation=45)',
                '                        plt.tight_layout()',
                '                        save_plot_to_adls(fig, f"{table_trend_folder}{feature}.png", dpi=150)',
                '                        plt.close(fig)',
                '                except:',
                '                    pass',
            ])
            
            # Update the print statement
            if j + 1 < len(lines) and 'print(f"  ✓ Saved' in lines[j + 1]:
                continue  # Skip old print, we'll add new one
    
    # Replace old print with new one
    source_17_new = '\n'.join(new_lines)
    source_17_new = source_17_new.replace(
        'print(f"  ✓ Saved monthly PSI trends")',
        'print(f"  ✓ Saved monthly PSI trends and plots")'
    )
    
    # Also add path definition at beginning
    source_17_new = source_17_new.replace(
        'MONTHLY_TRENDS_PLOT_PATH = PLOT_PATH + "monthly_trends/"',
        '''MONTHLY_TRENDS_PSI_PATH = PLOT_PATH + "monthly_trends_psi/"
dbutils.fs.mkdirs(MONTHLY_TRENDS_PSI_PATH)'''
    )
    
    nb['cells'][17]['source'] = source_17_new
    print("Updated cell 17 with PSI plots")

# Find and UPDATE: Monthly Statistics cell
for i, cell in enumerate(nb['cells']):
    source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
    
    if 'MONTHLY STATISTICS' in source and 'MEDIAN & AVERAGE' in source and cell['cell_type'] == 'code':
        print(f"Found Monthly Statistics at cell {i}")
        
        if 'table_stats_folder' not in source:
            print("Adding statistics trend plots...")
            
            lines = source.split('\n')
            new_lines = []
            
            for j, line in enumerate(lines):
                new_lines.append(line)
                
                if 'save_pandas_to_csv_adls(stats_df' in line and 'monthly_statistics_trends' in line:
                    new_lines.extend([
                        '',
                        '            # Create trend plots (median & average on same plot)',
                        '            MONTHLY_STATS_PLOT_PATH = PLOT_PATH + "monthly_statistics_trends/"',
                        '            dbutils.fs.mkdirs(MONTHLY_STATS_PLOT_PATH)',
                        '            table_stats_folder = f"{MONTHLY_STATS_PLOT_PATH}{table_name}/"',
                        '            dbutils.fs.mkdirs(table_stats_folder)',
                        '',
                        '            for feature in [f for f in num_features if f in df_spark.columns]:',
                        '                try:',
                        '                    median_data = stats_df[(stats_df[\'feature_name\'] == feature) & (stats_df[\'stat_method\'] == \'median\')]',
                        '                    average_data = stats_df[(stats_df[\'feature_name\'] == feature) & (stats_df[\'stat_method\'] == \'average\')]',
                        '',
                        '                    if len(median_data) > 0 and len(average_data) > 0:',
                        '                        month_cols = [col for col in stats_df.columns if col not in [\'feature_name\', \'stat_method\']]',
                        '                        median_values = median_data[month_cols].values[0]',
                        '                        average_values = average_data[month_cols].values[0]',
                        '',
                        '                        fig, ax = plt.subplots(figsize=(14, 6))',
                        '                        ax.plot(month_cols, median_values, marker=\'o\', linewidth=2, markersize=6,',
                        '                               color=\'steelblue\', label=\'Median\')',
                        '                        ax.plot(month_cols, average_values, marker=\'s\', linewidth=2, markersize=6,',
                        '                               color=\'coral\', label=\'Average\', linestyle=\'--\')',
                        '                        ax.set_title(f\'Monthly Statistics: {feature}\\\\n({table_name})\', fontsize=12, fontweight=\'bold\')',
                        '                        ax.set_ylabel(\'Value\', fontsize=10)',
                        '                        ax.set_xlabel(\'Month\', fontsize=10)',
                        '                        ax.legend(fontsize=10, loc=\'best\')',
                        '                        ax.grid(True, alpha=0.3)',
                        '                        ax.tick_params(axis=\'x\', rotation=45)',
                        '                        plt.tight_layout()',
                        '                        save_plot_to_adls(fig, f"{table_stats_folder}{feature}.png", dpi=150)',
                        '                        plt.close(fig)',
                        '                except:',
                        '                    pass',
                    ])
                    
                    # Update print
                    if j + 1 < len(lines) and 'print(f"  ✓ Saved monthly statistics")' in lines[j + 1]:
                        lines[j + 1] = '            print(f"  ✓ Saved monthly statistics and plots")'
            
            nb['cells'][i]['source'] = '\n'.join(new_lines)
            print(f"Updated cell {i} with statistics plots")
            break

# Save
with open('04_drift_analysis_psi.ipynb', 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=2)

print("\nNotebook fully updated!")
print(f"Final cell count: {len(nb['cells'])}")

