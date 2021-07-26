import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description="churn")
    parser.add_argument(
        "-d", "--date", dest="date", help="date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-e", "--env", dest="env", help="environment to execute in", required=True
    )
    parser.add_argument(
        "-l", "--layer", dest="layer", help="layer to sense", required=True
    )
    parser.add_argument(
        "-t", "--table", dest="table", help="table to sense", required=False
    )
    args = parser.parse_args()

    query = f"""
        select (CASE WHEN COUNT(1) >= 1 THEN 0 ELSE 1 END) as done
        from dwh_ml.log_etl_jobs
        where job_name like 'CTCT_FL_{args.table.upper()}_JB'
        and job_status = 'SUCCESFUL'
        and trunc(last_run_end) = trunc(CURRENT_DATE);
    """
    print(query)
    exit_code = 0


    if args.table:
        print(f"Table that is supposed to be read is {args.table.upper()}")
    else:
        print("Explanation found!")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()