import subprocess
import sys
import os

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# List of scripts to run in order
scripts = [
    "load_published_activities.py",
    "load_published_organisations.py",
    "load_phantom_activities.py",
    "load_phantom_organisations.py",
    "load_hierarchy_edges.py",
    "load_participation_edges.py",
    "load_financial_edges.py",
]

print("Starting sequential graph load process...")

for script in scripts:
    script_path = os.path.join(script_dir, script)
    print(f"--- Running {script} ---")
    try:
        # Ensure the script exists before trying to run it
        if not os.path.exists(script_path):
            print(f"Error: Script not found at {script_path}", file=sys.stderr)
            sys.exit(1)

        # Run the script using the same Python interpreter that is running this script
        # Pass current environment variables
        result = subprocess.run(
            [sys.executable, script_path],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
            env=os.environ,
            cwd=script_dir # Ensure script runs with its directory as CWD
        )
        print(f"Output from {script}:")
        print(result.stdout)
        print(f"--- Finished {script} ---")

    except subprocess.CalledProcessError as e:
        print(f"Error running {script}:", file=sys.stderr)
        print(f"Return code: {e.returncode}", file=sys.stderr)
        if e.stdout:
            print(f"Output: {e.stdout}", file=sys.stderr)
        print("Stopping execution due to error.", file=sys.stderr)
        sys.exit(1) # Exit if any script fails
    except Exception as e:
        print(f"An unexpected error occurred while trying to run {script}: {e}", file=sys.stderr)
        sys.exit(1)


print("--- All graph load scripts completed successfully! ---")
