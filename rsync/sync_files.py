import json
import os
import subprocess
import sys


def run_rsync(source, destination, remote_host, ssh_key_path, remote_user):
    """
    Runs the rsync command to synchronize files to a remote server.

    :param source: Source directory to sync
    :param destination: Remote destination directory
    :param remote_host: Remote server hostname or IP
    :param ssh_key_path: Path to the SSH private key
    :param remote_user: Remote server username
    """
    # Construct the rsync command
    rsync_command = [
        "rsync",
        "-avz",
        "--no-perms",
        "--omit-dir-times",
        "-e", f"ssh -i {ssh_key_path}",
        source,
        f"{remote_user}@{remote_host}:{destination}"
    ]

    try:
        # Run the rsync command
        result = subprocess.run(rsync_command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("rsync output:")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error running rsync:")
        print(e.stderr, file=sys.stderr)
        sys.exit(1)


def load_config(config_path):
    """
    Loads the configuration from a JSON file.

    :param config_path: Path to the configuration file
    :return: A dictionary containing configuration keys
    """
    try:
        with open(config_path, "r") as file:
            config = json.load(file)

            # Check for required fields
            required_fields = ["source", "destination", "remote_host", "ssh_key_path", "remote_user"]
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Configuration file must contain '{field}'.")

            return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError:
        print("Error: Configuration file is not a valid JSON.", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 sync_files.py <config_file>", file=sys.stderr)
        sys.exit(1)

    config_file = sys.argv[1]
    config_path = os.path.join("/code/data-processing/rsync/config_files", config_file)

    config = load_config(config_path)

    # Extract configuration values
    source = config["source"]
    destination = config["destination"]
    remote_host = config["remote_host"]
    ssh_key_path = config["ssh_key_path"]
    remote_user = config["remote_user"]

    # Run the rsync operation
    run_rsync(source, destination, remote_host, ssh_key_path, remote_user)


if __name__ == "__main__":
    main()
