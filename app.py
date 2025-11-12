import os
import csv
import openpyxl
from flask import Flask, render_template, request, send_file, redirect, url_for
from werkzeug.utils import secure_filename
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Side, Font
from collections import defaultdict
import pandas as pd

app = Flask(__name__)

# Add an upload size limit (adjust as needed). Prevent huge files that cause OOM.
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024  # 5 MB

activate = False
# Path for uploaded files
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def read_csv_headers(file_path):
    """Read CSV file and return all rows"""
    with open(file_path, mode='r', newline='', encoding='utf-8-sig') as file:
        csv_reader = csv.reader(file)
        rows = list(csv_reader)
    return rows


def create_header_dict(headers):
    """Create dictionary with header count and usage tracking"""
    header_count = defaultdict(int)
    for header in headers:
        header_count[header] += 1

    header_info = {}
    for header, count in header_count.items():
        header_info[header] = {
            'count': count,
            'used': 0
        }
    return header_info


def write_to_excel(rows, file_path):
    """Write data to an Excel file"""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    for row in rows:
        sheet.append(row)
    workbook.save(file_path)


def process_worksheet(df, sheet, start_row):
    """Process worksheet to calculate totals and averages"""
    columns_with_dot = [col for col in df.columns if '.' in col]
    prefix_groups = {}

    for col in columns_with_dot:
        prefix = col.split('.')[0]
        if prefix not in prefix_groups:
            prefix_groups[prefix] = []
        prefix_groups[prefix].append(col)

    last_row = start_row + len(df) + 1

    for prefix, cols in prefix_groups.items():
        total_sums = df[cols].sum()
        for col in cols:
            col_index = df.columns.get_loc(col) + 2  # Adjust for SlNo column
            total_sum = total_sums[col]
            sheet.cell(row=last_row, column=col_index, value=total_sum)

            num_values = df[col].notna().sum()
            average_value = total_sum / num_values if num_values > 0 else 0
            average_percentage = round((average_value / 5) * 100, 2)
            sheet.cell(row=last_row + 1, column=col_index, value=average_percentage)

        avg_of_avgs = df[cols].mean(axis=1).mean() / 5 * 100
        first_col_in_group = df.columns.get_loc(cols[0]) + 2
        sheet.cell(row=last_row + 2, column=first_col_in_group, value=round(avg_of_avgs, 2))


def add_slno_column(sheet, df):
    """Add serial number column"""
    sheet.insert_cols(1)
    sheet.cell(row=1, column=1, value='SlNo')
    for row_num in range(2, len(df) + 2):
        sheet.cell(row=row_num, column=1, value=row_num - 1)


def apply_borders(sheet, start_row, end_row, start_col, end_col):
    """Apply borders to a range of cells"""
    thin_border = Border(left=Side(style='thin'),
                         right=Side(style='thin'),
                         top=Side(style='thin'),
                         bottom=Side(style='thin'))
    for row in range(start_row, end_row + 1):
        for col in range(start_col, end_col + 1):
            cell = sheet.cell(row=row, column=col)
            cell.border = thin_border


def create_summary_sheet(workbook, df, file_path, branch_name):
    """Create summary sheet for each branch"""
    # Ensure we work on a copy to avoid SettingWithCopyWarning and accidental views
    df = df.copy()

    if f"{branch_name} - Summary" not in workbook.sheetnames:
        summary_sheet = workbook.create_sheet(f"{branch_name} - Summary")
    else:
        summary_sheet = workbook[f"{branch_name} - Summary"]

    # Extract filename information
    file_name = os.path.basename(file_path)
    file_name_without_extension = os.path.splitext(file_name)[0]
    parts = file_name_without_extension.split('_')
    
    if len(parts) >= 3:
        academic_year, department, semester_info = parts[0], parts[1], parts[2]
    else:
        academic_year = parts[0] if len(parts) > 0 else 'Unknown Year'
        department = parts[1] if len(parts) > 1 else 'Unknown Department'
        semester_info = parts[2] if len(parts) > 2 else 'Unknown Semester'

    class_year, semester_num = semester_info.split('-') if '-' in semester_info else ('Unknown Class', 'Unknown Semester')
    class_year_display = f"{class_year} B.Tech"
    semester_display = f"{semester_num} Semester"

    # Extract oldest feedback date
    if 'Timestamp' in df.columns:
        df['Timestamp'] = pd.to_datetime(df['Timestamp'].str.extract(r'(\d{4}/\d{2}/\d{2})')[0], errors='coerce')
        oldest_date = df['Timestamp'].min()
        oldest_date_str = oldest_date.strftime('%d-%m-%Y') if pd.notnull(oldest_date) else 'N/A'
    else:
        oldest_date_str = 'N/A'

    # Merge cells for header
    summary_sheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=11)
    summary_sheet.merge_cells(start_row=2, start_column=1, end_row=2, end_column=11)
    summary_sheet.merge_cells(start_row=3, start_column=1, end_row=3, end_column=11)
    summary_sheet.merge_cells(start_row=4, start_column=1, end_row=4, end_column=11)

    # Add header information
    summary_sheet.cell(row=1, column=1).value = "NARASARAOPETA ENGINEERING COLLEGE (AUTONOMOUS) - NARASARAOPET"
    summary_sheet.cell(row=2, column=1).value = f"STUDENT FEEDBACK SUMMARY - ACADEMIC YEAR - {academic_year}"
    summary_sheet.cell(row=3, column=1).value = f"DEPARTMENT OF {department}"
    summary_sheet.cell(row=4, column=1).value = f"CLASS: {class_year_display} {semester_display}, Section - {branch_name}, Date of Feedback: {oldest_date_str}"

    # Format header cells
    for row in range(1, 5):
        cell = summary_sheet.cell(row=row, column=1)
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.font = Font(bold=True)

    apply_borders(summary_sheet, start_row=1, end_row=4, start_col=1, end_col=11)

    # Process feedback data
    row_num = 5
    averages_dict = {}
    
    for col in df.columns:
        if col == 'Timestamp':
            continue
        if '.' in col:
            subject_name = col.split('.')[0]
            avg_value = pd.to_numeric(df[col], errors='coerce').mean()
            if pd.notnull(avg_value):
                avg_percentage = (avg_value * 100) / 5
                if subject_name not in averages_dict:
                    averages_dict[subject_name] = []
                averages_dict[subject_name].append(round(avg_percentage, 2))

    # Write feedback for each subject
    bold_font = Font(bold=True)
    for subject_name, avg_values in averages_dict.items():
        summary_sheet.merge_cells(start_row=row_num, start_column=1, end_row=row_num, end_column=8)
        summary_sheet.merge_cells(start_row=row_num, start_column=10, end_row=row_num, end_column=11)

        feedback_cell = summary_sheet.cell(row=row_num, column=1, value=f"Feedback for {subject_name}")
        feedback_cell.font = bold_font
        feedback_cell.alignment = Alignment(horizontal='left')

        responses_cell = summary_sheet.cell(row=row_num, column=10, value=f"No. of Responses {df.shape[0]}")
        responses_cell.font = bold_font
        responses_cell.alignment = Alignment(horizontal='right')

        apply_borders(summary_sheet, start_row=row_num, end_row=row_num, start_col=1, end_col=11)
        row_num += 1

        # Questions row
        questions = ["Questions", "Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10"]
        for col_num, question in enumerate(questions, start=1):
            cell = summary_sheet.cell(row=row_num, column=col_num, value=question)
            cell.font = bold_font
            cell.alignment = Alignment(horizontal='center', vertical='center')

        apply_borders(summary_sheet, start_row=row_num, end_row=row_num, start_col=1, end_col=11)
        row_num += 1

        # Percentage row
        summary_sheet.cell(row=row_num, column=1, value="Percentage").font = bold_font
        for col_num, value in enumerate(avg_values, start=2):
            summary_sheet.cell(row=row_num, column=col_num, value=value)

        apply_borders(summary_sheet, start_row=row_num, end_row=row_num, start_col=1, end_col=11)
        
        avg_of_avg = sum(avg_values) / len(avg_values) if avg_values else 0
        summary_sheet.cell(row=row_num + 1, column=1, value="Feedback").font = bold_font
        summary_sheet.cell(row=row_num + 1, column=2, value=round(avg_of_avg, 2))
        apply_borders(summary_sheet, start_row=row_num + 1, end_row=row_num + 1, start_col=1, end_col=11)
        row_num += 2

    workbook.save(file_path)


def process_all_sheets(df, workbook, unique_column, file_path):
    """Process all sheets based on unique column values"""
    sheet = workbook.active
    add_slno_column(sheet, df)
    process_worksheet(df, sheet, start_row=1)

    if unique_column in df.columns:
        unique_values = df[unique_column].unique()
        for value in unique_values:
            if value not in workbook.sheetnames:
                new_sheet = workbook.create_sheet(title=str(value))
                filtered_data = df[df[unique_column] == value]
                add_slno_column(new_sheet, filtered_data)

                for col_num, column_title in enumerate(filtered_data.columns, start=2):
                    new_sheet.cell(row=1, column=col_num, value=column_title)

                for row_num, row_data in enumerate(filtered_data.values, start=2):
                    for col_num, cell_value in enumerate(row_data, start=2):
                        new_sheet.cell(row=row_num, column=col_num, value=cell_value)

                process_worksheet(filtered_data, new_sheet, start_row=1)
                create_summary_sheet(workbook, filtered_data, file_path, branch_name=str(value))


def create_comments_sheet(workbook, df):
    """Create Comments worksheet"""
    if 'Comments' not in workbook.sheetnames:
        comments_sheet = workbook.create_sheet('Comments')
        unique_sections = sorted(df['SECTION'].unique())
        
        border = Border(left=Side(style='thin'),
                        right=Side(style='thin'),
                        top=Side(style='thin'),
                        bottom=Side(style='thin'))

        for i, section in enumerate(unique_sections):
            comments_sheet.merge_cells(start_row=1, start_column=i * 2 + 1, end_row=1, end_column=i * 2 + 2)
            comments_sheet.cell(row=1, column=i * 2 + 1).value = section
            comments_sheet.cell(row=2, column=i * 2 + 1).value = "Comments about the Department"
            comments_sheet.cell(row=2, column=i * 2 + 2).value = "Any Suggestions about the College"
            comments_sheet.cell(row=2, column=i * 2 + 1).font = Font(bold=True)
            comments_sheet.cell(row=2, column=i * 2 + 2).font = Font(bold=True)

        last_two_columns = df.columns[-2:]
        for i, section in enumerate(unique_sections):
            section_values = df[df['SECTION'] == section][last_two_columns].values
            for row_index, value in enumerate(section_values):
                comments_sheet.cell(row=row_index + 3, column=i * 2 + 1, value=value[0])
                comments_sheet.cell(row=row_index + 3, column=i * 2 + 2, value=value[1])

        for row in comments_sheet.iter_rows():
            for cell in row:
                cell.border = border


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    # Reject uploads larger than MAX_CONTENT_LENGTH early
    if request.content_length and request.content_length > app.config['MAX_CONTENT_LENGTH']:
        # You can render a message or flash; keep simple and redirect
        return redirect(url_for('index'))

    if 'file' not in request.files:
        return redirect(request.url)

    file = request.files['file']
    if file.filename == '':
        return redirect(request.url)

    if file:
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        # Read CSV and strip header spaces
        rows = read_csv_headers(file_path)
        rows[0] = [header.strip() for header in rows[0]]

        # Create header dictionary
        header_info = create_header_dict(rows[0])

        # Generate new column names with suffixes for duplicates
        generated_columns = []
        for header in rows[0]:
            if header_info[header]["count"] > 1:
                header_info[header]["used"] += 1
                new_column_name = f"{header}.{header_info[header]['used']}"
                generated_columns.append(new_column_name)
            else:
                generated_columns.append(header)

        rows[0] = generated_columns

        # Create output Excel file
        file_base, _ = os.path.splitext(file_path)
        excel_path = f"{file_base}_processed.xlsx"

        write_to_excel(rows, excel_path)

        # Load and process Excel file
        workbook = load_workbook(excel_path)
        main_df = pd.read_excel(excel_path, sheet_name=0)

        # Work on an explicit copy to avoid view-related warnings and ensure independent memory
        main_df = main_df.copy()

        unique_column = 'SECTION'
        process_all_sheets(main_df, workbook, unique_column, excel_path)
        create_comments_sheet(workbook, main_df)

        # Sort sheets
        sheets = workbook.worksheets
        if len(sheets) > 2:
            sorted_middle_sheets = sorted(sheets[1:-1], key=lambda ws: ws.title)
            workbook._sheets = [sheets[0]] + sorted_middle_sheets + [sheets[-1]]

        workbook.save(excel_path)

        try:
            # Send file for download
            response = send_file(excel_path, as_attachment=True)
            
            # Delete uploaded CSV file after processing
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    print(f"Deleted uploaded file: {file_path}")
            except Exception as delete_error:
                print(f"Could not delete file {file_path}: {delete_error}")
            
            return response
        except Exception as e:
            print(f"Error: {e}")
            # Clean up both files on error
            if os.path.exists(file_path):
                os.remove(file_path)
            if os.path.exists(excel_path):
                os.remove(excel_path)
            return redirect(url_for('index'))
    
    return redirect(url_for('index'))


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
