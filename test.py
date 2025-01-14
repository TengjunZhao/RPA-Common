from datetime import datetime, timedelta


def calculate_year(week, reference_date):
    """
    Calculate the year of the given ISO week number based on a reference date.
    Handles edge cases for weeks that belong to the previous or next year.
    """
    try:
        # Convert reference date to datetime
        reference_date = datetime.strptime(reference_date, '%Y-%m-%d')

        # Construct the first day of the given ISO week
        year = reference_date.year
        first_day_of_week = datetime.strptime(f'{year}-W{int(week)}-1', "%Y-W%U-%w")

        # Adjust for the first week of the next year
        if week == '01' and reference_date.month == 12:
            year += 1
        # Adjust for the last weeks of the previous year
        elif week in ['52', '53'] and first_day_of_week.month == 1:
            year -= 1

        return year
    except Exception as e:
        print(f"Error calculating year for week {week}: {e}")
        return None


if __name__ == '__main__':
     reference_date = datetime.now().strftime('%Y-%m-%d')  # Use today's date as a reference
     year = calculate_year('01', '2024-12-31')
     print(year)