#!/bin/sh
########################################################################################################################
# snail_backup.sh - SNAIL BACKUP SCRIPT FOR FREEBSD SYSTEMS
########################################################################################################################
# This script is a robust, rsync-based backup utility designed for FreeBSD systems running in sh shell environment.
# It provides automated, incremental backups with support for local/remote targets, auto-mount/unmount of dedicated
# backup storage devices, and automatic cleanup of failed/old backups and old logs.
#
# KEY FEATURES:
#   1. Incremental backups using rsync --link-dest for space efficiency
#   2. Automatic mounting/unmounting of backup storage devices (minimizes exposure risk)
#   3. SSH integration for remote backups (custom ports/key files supported)
#   4. PID file locking to prevent concurrent backup job execution
#   5. Automatic cleanup of failed backups and rotation of old backups/logs
#   6. Comprehensive logging with configurable log retention
#   7. Free space checking before backup execution
#   8. Strict permission checks for configuration/SSH key files
#
# USAGE SCENARIOS:
#   - Local system backups to dedicated storage devices
#   - Remote backups to/from other FreeBSD/Linux systems via SSH
#   - Scheduled backups (daily/weekly/monthly) via cron
#   - System-level backups with exclusion of non-essential directories
#
# DEPENDENCIES:
#   - Core FreeBSD utilities (awk, basename, cat, chmod, cut, date, df, find, grep, head, ls, mkdir, mount, nice, ps,
#     renice, rm, rsync, sed, sort, ssh, stat, tail, touch, tr, umount, wc)
#   - rsync (installed via pkg or ports)
#   - SSH client (for remote backups)
#
# SECURITY CONSIDERATIONS:
#   - Must be run as root (UID 0) for system-level access and mount/unmount operations
#   - Configuration files should have 400/600 permissions (strictly enforced)
#   - SSH key files must have 400/600 permissions (strictly enforced)
#   - Log/PID directories use 700 permissions to restrict access
#   - Auto-mount minimizes device exposure time (reduces ransomware risk)
#
# USAGE GUIDE:
#   1. Create a configuration file from snail_backup.conf.template (recommended path: /usr/local/etc/snail/)
#   2. Set appropriate permissions on config file: chmod 600 /usr/local/etc/snail/your_config.conf
#   3. Test basic functionality: ./snail_backup.sh -c /usr/local/etc/snail/your_config.conf --self-check
#   4. Run manual backup: ./snail_backup.sh -c /usr/local/etc/snail/your_config.conf -j job_id
#   5. Schedule via cron (example for daily backup at 2 AM):
#      0 2 * * * /usr/local/sbin/snail_backup.sh -c /usr/local/etc/snail/your_config.conf -j job_id -a -r 30
#
# IMPORTANT NOTES:
#   - Always test backups and restoration procedures before production use
#   - Verify backup integrity regularly (TODO: implement --verify option)
#   - Keep configuration files secure (restrict access to root only)
#   - Use unique JOB_IDENTIFIER values for different backup schedules
#   - Monitor log files for backup failures (/var/log/snail_backup/<job_identifier>/)
#   - Ensure sufficient free space on target storage before backup execution
########################################################################################################################

# Static configuration variable
VERSION="1.0.0"                                                 # Script version definition
LOG_ROOT_FOLDER="/var/log/snail_backup"                         # Log storage directory
PID_ROOT_FOLDER="/var/run/snail_backup"                         # Folder of PID file to prevent concurrent runs
CONFIG_FILE_FOLDERS="/usr/local/etc/snail /etc/snail"           # Search path if relative path specified
JOB_PROCESSING="_job_is_processing_"                            # A file exist in target folder indicate job not finished

# Dynamic variable declaration depends on job identifier
JOB_IDENTIFIER=""
LOG_FILE=""
PID_FILE=""
TARGET_FOLDER=""
LAST_BACKUP_FOLDER=""

# Other dynamic variable
EXCLUDE_LIST="--exclude=\"/$JOB_PROCESSING\""
SSH_COMMAND=""
START_TIME=""

# ----------------------------------------------------------------------------------------------------------------------
# log() - CENTRALIZED LOGGING FUNCTION
# ----------------------------------------------------------------------------------------------------------------------
# This function handles all logging operations for the script, writing to both log file and console output.
# It supports different log levels (DEBUG, INFO, WARNING, ERROR) with appropriate stream redirection (stdout/stderr).
# Log entries include timestamp, log level, and custom message for auditability.
#
# PARAMETERS:
#   $1 - Log level (DEBUG, INFO, WARNING, ERROR)
#   $* - Log message (remaining arguments)
#
# OUTPUT:
#   Writes formatted log entry to LOG_FILE (if set) and appropriate output stream (stdout for DEBUG/INFO, stderr for
#   WARNING/ERROR)
# ----------------------------------------------------------------------------------------------------------------------
log() {
    # Extract log level from first argument
    local level="$1"
    # Shift arguments to get the log message (remaining parameters)
    shift
    local message="$*"
    # Generate ISO-like timestamp for log entry
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")

    # Format log entry with timestamp and level
    local log_entry="[${timestamp}] [${level}] $message"

    # Write to log file if log file path is defined
    if [ -n "$LOG_FILE" ]; then
        echo "$log_entry" >> "$LOG_FILE"
    fi

    # Output to appropriate stream based on log level
    # WARNING/ERROR go to stderr; all others to stdout
    case "$level" in
        WARNING|ERROR)
            echo "$log_entry" >&2
            ;;
        *)
            echo "$log_entry"
            ;;
    esac
}

# ----------------------------------------------------------------------------------------------------------------------
# log_debug() - DEBUG LEVEL LOGGING WRAPPER
# ----------------------------------------------------------------------------------------------------------------------
# Wrapper function for log() that sets log level to DEBUG
#
# PARAMETERS:
#   $* - Debug message
# ----------------------------------------------------------------------------------------------------------------------
log_debug() {
    # Pass all arguments to log function with DEBUG level
    log "DEBUG" "$@"
}

# ----------------------------------------------------------------------------------------------------------------------
# log_info() - INFO LEVEL LOGGING WRAPPER
# ----------------------------------------------------------------------------------------------------------------------
# Wrapper function for log() that sets log level to INFO
#
# PARAMETERS:
#   $* - Informational message
# ----------------------------------------------------------------------------------------------------------------------
log_info() {
    # Pass all arguments to log function with INFO level
    log "INFO" "$@"
}

# ----------------------------------------------------------------------------------------------------------------------
# log_warning() - WARNING LEVEL LOGGING WRAPPER
# ----------------------------------------------------------------------------------------------------------------------
# Wrapper function for log() that sets log level to WARNING (output to stderr)
#
# PARAMETERS:
#   $* - Warning message
# ----------------------------------------------------------------------------------------------------------------------
log_warning() {
    # Pass all arguments to log function with WARNING level
    log "WARNING" "$@"
}

# ----------------------------------------------------------------------------------------------------------------------
# log_error() - ERROR LEVEL LOGGING WRAPPER
# ----------------------------------------------------------------------------------------------------------------------
# Wrapper function for log() that sets log level to ERROR (output to stderr)
#
# PARAMETERS:
#   $* - Error message
# ----------------------------------------------------------------------------------------------------------------------
log_error() {
    # Pass all arguments to log function with ERROR level
    log "ERROR" "$@"
}

# ----------------------------------------------------------------------------------------------------------------------
# check_commands() - DEPENDENCY VALIDATION FUNCTION
# ----------------------------------------------------------------------------------------------------------------------
# Verifies that all required system commands are available in the PATH.
# Logs OK/FAILED status for each command and compiles list of missing commands (if any).
# This function is typically called via --self-check option for pre-deployment validation.
# ----------------------------------------------------------------------------------------------------------------------
check_commands() {
    # Initialize empty string to track missing commands
    local missing_commands=""

    # Iterate over list of required commands
    for cmd in awk basename cat chmod cut date df find grep head lockf ls mkdir mount nice ps renice rm rsync sed sort ssh stat tail touch tr umount wc; do
        # Check if command exists in PATH
        if command -v "$cmd" >/dev/null 2>&1; then
            log_info "check command $cmd ... OK"
        else
            # Log warning and add to missing commands list
            log_warning "check command $cmd ... FAILED"
            missing_commands="$missing_commands $cmd"
        fi
    done

    # Log summary of missing commands if any
    if [ -n "$missing_commands" ]; then
        log_warning "Missing commands:$missing_commands"
    fi
}

# ----------------------------------------------------------------------------------------------------------------------
# usage() - USAGE INFORMATION FUNCTION
# ----------------------------------------------------------------------------------------------------------------------
# Displays formatted usage information, command line options, and examples for the script.
# Automatically detects script name and displays version information.
# Called when:
#   - No arguments are provided
#   - -h/--help option is used
#   - Invalid arguments are provided
#   - Configuration file is missing
# ----------------------------------------------------------------------------------------------------------------------
usage() {
    # Get script name (without path) for usage message
    local script_name=$(basename "$0")
    # Print formatted usage information using here-doc
    cat <<- EOF

	${script_name} version ${VERSION}

	usage:
	    ${script_name} <-h|--help>
	    ${script_name} --check-command
	    ${script_name} -c <config-file> [override options]

	options:
	    -h, --help                      Show this usage
	    -s, --self-check                Check if required commands are available
	    -c, --config-file <file>        Configuration file (required), search /usr/local/etc/snail and /etc/snail folder
	                                    if relative path specified
	    -j, --job-identifier <id>       Job identifier, use for backup, log folder and pid file name (^[a-zA-Z0-9_]*$)
	    -a, --auto-clean <true|false>   Enable or disable automatically clean up failure backups, old backups and logs
	    -r, --retain-count <number>     Numbers of backups and logs to retain while automatically cleaning up

	examples:
	    ${script_name} -c system-backup.conf
	    ${script_name} -c home-backup.conf --retain-full 3
	    ${script_name} -c daily.conf --job-identifier daily --increase-count 10 --retain-full 3

	EOF
}

# ----------------------------------------------------------------------------------------------------------------------
# find_config_file() - CONFIGURATION FILE RESOLVER
# ----------------------------------------------------------------------------------------------------------------------
# Resolves absolute path for configuration file by:
#   1. Checking if provided path is absolute and exists
#   2. Searching predefined CONFIG_FILE_FOLDERS if relative path is provided
#
# PARAMETERS:
#   $1 - Configuration file path (relative or absolute)
#
# RETURN:
#   Writes resolved absolute path to stdout if found
#   Returns 0 on success, 1 on failure
# ----------------------------------------------------------------------------------------------------------------------
find_config_file() {
    # Get input config file path from argument
    local config_file="$1"
    # Initialize empty variable to store found file path
    local found_file=""

    # Check if path is absolute (starts with /)
    if [ "${config_file#/}" != "$config_file" ]; then
        # Absolute path - check if file exists
        if [ -f "$config_file" ]; then
            echo "$config_file"
            return 0
        else
            return 1
        fi
    fi

    # Relative path - search predefined folders
    local folder
    for folder in $CONFIG_FILE_FOLDERS; do
        local potential_file="$folder/$config_file"
        # Check if file exists in current search folder
        if [ -f "$potential_file" ]; then
            found_file="$potential_file"
            break
        fi
    done

    # Return found file path if exists
    if [ -n "$found_file" ]; then
        echo "$found_file"
        return 0
    else
        return 1
    fi
}

# ----------------------------------------------------------------------------------------------------------------------
# parse_arguments() - COMMAND LINE ARGUMENT PARSER
# ----------------------------------------------------------------------------------------------------------------------
# Parses and validates all command line arguments passed to the script.
# Key responsibilities:
#   1. Handles help/self-check options
#   2. Resolves and loads configuration file (with permission checks)
#   3. Validates and applies override options (job identifier, auto-clean, retain count)
#   4. Exits with error/usage info for invalid arguments
#
# PARAMETERS:
#   $@ - All command line arguments passed to the script
# ----------------------------------------------------------------------------------------------------------------------
parse_arguments() {
    # Show usage if no arguments are provided
    [ $# -eq 0 ] && {
        usage
        exit 0
    }

    # Initialize empty variable for resolved config file path
    local config_file=""

    # Parse command line arguments using while loop
    while [ $# -gt 0 ]; do
        case "$1" in
        -h|--help)
            # Show usage and exit successfully
            usage
            exit 0
            ;;

        -s|--self-check)
            # Run command dependency check and exit
            check_commands
            exit 0
            ;;

        -c|--config-file)
            # Resolve config file path using find_config_file function
            config_file=$(find_config_file "$2")
            if [ $? -ne 0 ]; then
                log_error "Cannot find config file: $2"
                usage
                exit 1
            fi

            # Check if config file is readable
            if [ ! -r "$config_file" ]; then
                log_error "Config file is not readable: $config_file"
                exit 1
            fi

            # Get file permissions in octal format (FreeBSD stat format)
            local key_perms=$(stat -f "%Lp" "$config_file" 2>/dev/null)
            # Enforce strict permissions (400 or 600) for security
            [ "$key_perms" -eq 400 ] || [ "$key_perms" -eq 600 ] || {
                log_error "Config file permissions too open: $config_file (0$key_perms)"
                exit 1
            }

            # Load configuration file into current shell environment
            . "$config_file"
            if [ $? -ne 0 ]; then
                log_error "Parse config file error: $config_file"
                exit 1
            fi

            log_info "Loaded config file $config_file"
            # Shift past -c and config file path
            shift 2
            ;;

        -j|--job-identifier)
            # Validate job identifier format (only letters, numbers, underscores)
            if ! echo "$2" | grep -q -E '^[a-zA-Z0-9_]+$'; then
                log_error "Illegal job identifier: $2, only a-z, A-Z, 0-9 and '_' are allowed"
                exit 1
            fi

            # Set job identifier from command line override
            JOB_IDENTIFIER="$2"
            log_info "Override job identifier to $JOB_IDENTIFIER"
            shift 2
            ;;

        -a|--auto-clean)
            # Validate auto-clean value (must be true or false)
            [ "$2" != "true" ] && [ "$2" != "false" ] && {
                log_error "Invalid parameter value: '$2' (must be 'true' or 'false')"
                exit 1
            }

            # Set auto-clean flag from command line override
            ENABLE_AUTO_CLEAN="$2"
	    log_info "Override enable automatiocally clean backups and logs to $ENABLE_AUTO_CLEAN"
            shift 2
            ;;

        -r|--retain-count)
            # Validate retain count is positive integer
            if ! echo "$2" | grep -q -E '^[1-9][0-9]*$'; then
                log_error "Retain count must be a positive number"
                exit 1
            fi

            # Set retain count from command line override
            RETAIN_COUNT="$2"
	    log_info "Override retain count of old backups and logs with $RETAIN_COUNT"
            shift 2
            ;;

        *)
            # Handle unexpected arguments
            log_error "Unexpected argument: $1"
            usage
            exit 1
            ;;
        esac
    done

    # Enforce config file requirement (must be specified with -c/--config-file)
    if [ -z "$config_file" ]; then
        log_error "Config file must be specified with -c or --config-file"
        usage
        exit 1
    fi
}

# ----------------------------------------------------------------------------------------------------------------------
# check_config() - CONFIGURATION VALIDATION FUNCTION
# ----------------------------------------------------------------------------------------------------------------------
# Performs comprehensive validation of configuration parameters loaded from config file/command line overrides.
# Validates:
#   - Boolean parameters (ENABLE_AUTOMOUNT, ENABLE_SSH, ENABLE_AUTO_CLEAN)
#   - Required parameters for auto-mount (MOUNT_DEVICE, MOUNT_POINT)
#   - SSH configuration (port range, key file existence/permissions)
#   - Source/target folder non-emptiness
#   - Job identifier format and non-emptiness
#   - Numeric parameters (MIN_FREE_SPACE_KB)
#
# Exits with error code 1 if any validation fails.
# ----------------------------------------------------------------------------------------------------------------------
check_config() {
    log_info "Checking configuration variables..."

    # Validate ENABLE_AUTOMOUNT is boolean (true/false)
    [ -n "$ENABLE_AUTOMOUNT" ] && [ "$ENABLE_AUTOMOUNT" != "true" ] && [ "$ENABLE_AUTOMOUNT" != "false" ] && {
        log_error "ENABLE_AUTOMOUNT must be 'true' or 'false'"
        exit 1
    }

    # Validate required mount parameters if automount is enabled
    [ "$ENABLE_AUTOMOUNT" = "true" ] && [ -z "$MOUNT_DEVICE" ] && {
        log_error "MOUNT_DEVICE cannot be empty while ENABLE_AUTOMOUNT is true"
        exit 1
    }

    [ "$ENABLE_AUTOMOUNT" = "true" ] && [ -z "$MOUNT_POINT" ] && {
        log_error "MOUNT_POINT cannot be empty while ENABLE_AUTOMOUNT is true"
        exit 1
    }

    # Validate ENABLE_SSH is boolean (true/false)
    [ -n "$ENABLE_SSH" ] && [ "$ENABLE_SSH" != "true" ] && [ "$ENABLE_SSH" != "false" ] && {
        log_error "ENABLE_SSH must be 'true' or 'false'"
        exit 1
    }

    # Validate SSH configuration if SSH is enabled
    [ "$ENABLE_SSH" = "true" ] && {
        # Validate SSH port is integer between 1 and 65535
        [ -n "$SSH_PORT" ] && ! (echo "$SSH_PORT" | grep -q -E '^[0-9]+$' && [ "$SSH_PORT" -ge 1 ] && [ "$SSH_PORT" -le 65535 ]) && {
            log_error "SSH_PORT must be an integer between 1 and 65535 when ENABLE_SSH is true"
            exit 1
        }

        # Validate SSH key file if specified
        [ -n "$SSH_KEY_FILE" ] && {
            [ ! -f "$SSH_KEY_FILE" ] || [ ! -r "$SSH_KEY_FILE" ] && {
                log_error "SSH_KEY_FILE does not exist or is not readable"
                exit 1
            }

            # Enforce strict permissions for SSH key file (400 or 600)
            local key_perms=$(stat -f "%Lp" "$SSH_KEY_FILE" 2>/dev/null)
            [ "$key_perms" -ne 400 ] && [ "$key_perms" -ne 600 ] && {
                log_error "SSH key file permissions $key_perms is too open, should be 600 or 400"
                exit 1
            }
        }
    }

    # Validate SOURCE_FOLDER is not empty
    [ -z "$SOURCE_FOLDER" ] && {
        log_error "SOURCE_FOLDER cannot be empty"
        exit 1
    }

    # Validate TARGET_ROOT_FOLDER is not empty
    [ -z "$TARGET_ROOT_FOLDER" ] && {
        log_error "TARGET_ROOT_FOLDER cannot be empty"
        exit 1
    }

    # Validate MIN_FREE_SPACE_KB is non-negative integer (or empty)
    [ -n "$MIN_FREE_SPACE_KB" ] && ! echo "$MIN_FREE_SPACE_KB" | grep -q -E '^[0-9]+$' && {
        log_error "MIN_FREE_SPACE_KB must be empty or a non-negative integer"
        exit 1
    }

    # Validate JOB_IDENTIFIER is not empty
    [ -z "$JOB_IDENTIFIER" ] && {
        log_error "Job identifier must be set, see JOB_IDENTIFIER in config file and --job-identifier command line option"
        exit 1
    }

    # Validate JOB_IDENTIFIER format (only letters, numbers, underscores)
    echo "$JOB_IDENTIFIER" | grep -q -E '^[a-zA-Z0-9_]+$' || {
        log_error "JOB_IDENTIFIER can only contain letters, numbers and underscores"
        exit 1
    }

    # Validate ENABLE_AUTO_CLEAN is boolean (true/false)
    [ -n "$ENABLE_AUTO_CLEAN" ] && [ "$ENABLE_AUTO_CLEAN" != "true" ] && [ "$ENABLE_AUTO_CLEAN" != "false" ] && {
        log_error "ENABLE_AUTO_CLEAN must be 'true' or 'false'"
        exit 1
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# init_timestamp() - TIMESTAMP INITIALIZATION
# ----------------------------------------------------------------------------------------------------------------------
# Generates a timestamp in YYYYMMDD_HHMMSS format for backup folder naming and log file naming.
# Exits with error code 1 if timestamp generation fails (unlikely but handled for robustness).
# ----------------------------------------------------------------------------------------------------------------------
init_timestamp() {
    # Generate timestamp with format YYYYMMDD_HHMMSS (safe for file/directory names)
    TIMESTAMP=$(date +%Y%m%d_%H%M%S) || {
        log_error "Failed to get timestamp"
        exit 1
    }

    log_info "Lock timestamp to $TIMESTAMP"
}

# ----------------------------------------------------------------------------------------------------------------------
# init_log() - LOG FILE INITIALIZATION
# ----------------------------------------------------------------------------------------------------------------------
# Initializes log directory and log file for the current backup job:
#   1. Creates job-specific log directory (with 700 permissions)
#   2. Creates timestamped log file (with 600 permissions)
#   3. Sets LOG_FILE variable to path of created log file
#
# Exits with error code 1 if any directory/file operation fails.
# ----------------------------------------------------------------------------------------------------------------------
init_log() {
    log_info "Initialize log environment..."

    # Create job-specific log directory (mkdir -p to handle nested directories)
    mkdir -p "$LOG_ROOT_FOLDER/$JOB_IDENTIFIER" || {
        log_error "Failed to create log folder: $LOG_ROOT_FOLDER/$JOB_IDENTIFIER"
        exit 1
    }

    # Set restrictive permissions (700) on log directory (only root access)
    chmod 700 "$LOG_ROOT_FOLDER/$JOB_IDENTIFIER" || {
        log_error "Failed to set permissions to 700 on log folder: $LOG_ROOT_FOLDER/$JOB_IDENTIFIER"
        exit 1
    }

    # Define full path to timestamped log file
    LOG_FILE="$LOG_ROOT_FOLDER/$JOB_IDENTIFIER/${TIMESTAMP}.log"
    # Create empty log file
    touch "$LOG_FILE" || {
        log_error "Failed to touch log file: $LOG_FILE"
        exit 1
    }
    # Set restrictive permissions (600) on log file (only root read/write)
    chmod 600 "$LOG_FILE" || {
        log_error "Failed to set permissions on log file: $LOG_FILE"
        exit 1
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# mount_device() - AUTOMATIC DEVICE MOUNTING
# ----------------------------------------------------------------------------------------------------------------------
# Handles automatic mounting of backup storage device when ENABLE_AUTOMOUNT=true:
#   1. Skips if automount is disabled
#   2. Creates mount point if it doesn't exist
#   3. Checks if device is already mounted (skips if true)
#   4. Mounts device with specified filesystem type (if provided)
#
# Exits with error code 1 if mount operation fails.
# ----------------------------------------------------------------------------------------------------------------------
mount_device() {
    # Skip if automount is disabled in config
    [ "$ENABLE_AUTOMOUNT" != "true" ] && {
        log_info "Automount is disabled (ENABLE_AUTOMOUNT=false), skip mounting"
        return 0
    }

    # Create mount point directory if it doesn't exist
    [ ! -d "$MOUNT_POINT" ] && {
        log_info "Mount point $MOUNT_POINT does not exist, creating..."
        mkdir -p "$MOUNT_POINT" || {
            log_error "Failed to create mount point $MOUNT_POINT"
            exit 1
        }
    }

    # Check if device is already mounted at the specified mount point
    if mount | grep -q "^$MOUNT_DEVICE on $MOUNT_POINT "; then
        log_info "Device $MOUNT_DEVICE is already mounted on $MOUNT_POINT"
        return 0
    fi

    # Build mount command (include filesystem type if specified)
    local mount_cmd
    [ -n "$MOUNT_FS_TYPE" ] && mount_cmd="mount -t $MOUNT_FS_TYPE $MOUNT_DEVICE $MOUNT_POINT" ||
        mount_cmd="mount $MOUNT_DEVICE $MOUNT_POINT"

    log_info "Executing mount command: $mount_cmd"

    # Execute mount command (suppress stderr for cleaner logging)
    eval "$mount_cmd" 2>/dev/null && {
        log_info "Successfully mounted $MOUNT_DEVICE to $MOUNT_POINT"
        return 0
    } || {
        log_error "Failed to mount $MOUNT_DEVICE to $MOUNT_POINT"
        exit 1
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# umount_device() - AUTOMATIC DEVICE UNMOUNTING
# ----------------------------------------------------------------------------------------------------------------------
# Handles automatic unmounting of backup storage device when ENABLE_AUTOMOUNT=true:
#   1. Skips if automount is disabled
#   2. Checks if mount point is mounted (skips if false)
#   3. Unmounts the mount point
#
# Exits with error code 1 if unmount operation fails.
# ----------------------------------------------------------------------------------------------------------------------
umount_device() {
    # Skip if automount is disabled in config
    [ "$ENABLE_AUTOMOUNT" != "true" ] && {
        log_info "Automount is disabled (ENABLE_AUTOMOUNT=false), skip unmounting"
        return 0
    }

    # Skip if mount point is not currently mounted
    ! mount | grep -qF "$MOUNT_POINT" && {
        log_info "Mount point $MOUNT_POINT is not mounted, skip unmounting"
        return 0
    }

    # Build unmount command (target mount point, not device)
    local umount_cmd="umount $MOUNT_POINT"
    log_info "Executing unmount command: $umount_cmd"

    # Execute unmount command (suppress stderr for cleaner logging)
    eval "$umount_cmd" 2>/dev/null && {
        log_info "Successfully unmounted $MOUNT_POINT"
        return 0
    } || {
        log_error "Failed to unmount $MOUNT_POINT"
        exit 1
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# build_exclude_list() - EXCLUDE DIRECTORY LIST BUILDER
# ----------------------------------------------------------------------------------------------------------------------
# Builds rsync-compatible exclude list from EXCLUDE_DIRS configuration parameter:
#   1. Skips if EXCLUDE_DIRS is empty
#   2. Processes each line (trims whitespace, skips empty lines)
#   3. Builds EXCLUDE_LIST variable with --exclude parameters for rsync
#   4. Uses set -f to disable globbing during processing
# ----------------------------------------------------------------------------------------------------------------------
build_exclude_list() {
    log_info "Build exclude directory list..."

    # Skip if no exclude directories are defined
    [ -z "$EXCLUDE_DIRS" ] && return 0

    # Disable globbing (pathname expansion) to handle spaces/special characters
    set -f

    # Process each line in EXCLUDE_DIRS
    while read -r line; do
        # Trim leading/trailing whitespace from line
        line_processed=$(printf '%s' "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
        # Skip empty lines after trimming
        [ -z "$line_processed" ] && continue

        # Build rsync --exclude parameter list
        if [ -z "$EXCLUDE_LIST" ]; then
            EXCLUDE_LIST="--exclude=\"$line_processed\""
        else
            EXCLUDE_LIST="$EXCLUDE_LIST --exclude=\"$line_processed\""
        fi
    done <<- EOF
		$EXCLUDE_DIRS
	EOF

    # Re-enable globbing after processing
    set +f
}

# ----------------------------------------------------------------------------------------------------------------------
# build_ssh_command() - SSH COMMAND BUILDER
# ----------------------------------------------------------------------------------------------------------------------
# Builds custom SSH command string for rsync when ENABLE_SSH=true:
#   1. Skips if SSH is disabled
#   2. Starts with base "ssh" command
#   3. Adds port parameter (-p) if SSH_PORT is set
#   4. Adds key file parameter (-i) if SSH_KEY_FILE is set
#
# Result is stored in SSH_COMMAND variable for use in rsync command.
# ----------------------------------------------------------------------------------------------------------------------
build_ssh_command() {
    # Skip if SSH is disabled in config
    if [ "$ENABLE_SSH" = "false" ]; then
        log_info "SSH is disabled (ENABLE_SSH=false)"
        return 0
    fi

    log_info "SSH is enabled (ENABLE_SSH=true), building SSH command..."
    # Base SSH command
    SSH_COMMAND="ssh"

    # Add port parameter if SSH_PORT is defined
    if [ -n "$SSH_PORT" ]; then
        SSH_COMMAND="$SSH_COMMAND -p $SSH_PORT"
    fi

    # Add key file parameter if SSH_KEY_FILE is defined
    if [ -n "$SSH_KEY_FILE" ]; then
        SSH_COMMAND="$SSH_COMMAND -i $SSH_KEY_FILE"
    fi
}

# ----------------------------------------------------------------------------------------------------------------------
# prepare_job() - BACKUP JOB PREPARATION
# ----------------------------------------------------------------------------------------------------------------------
# Prepares environment for backup job execution:
#   1. Creates job-specific target directory
#   2. Identifies last successful backup (for --link-dest incremental backup)
#   3. Builds exclude list (via build_exclude_list())
#   4. Builds SSH command (via build_ssh_command())
#   5. Creates timestamped target folder for current backup
#
# Exits with error code 1 if directory creation fails.
# ----------------------------------------------------------------------------------------------------------------------
prepare_job() {
    # Define job-specific target root directory
    local job_folder="$TARGET_ROOT_FOLDER/$JOB_IDENTIFIER"

    # Create job directory (mkdir -p to handle nested directories)
    log_info "Create backup job directory if necessary: $job_folder"
    mkdir -p "$job_folder" || {
        log_error "Failed to create target job folder: $job_folder"
        exit 1
    }

    # Find all successful (completed) backup directories:
    # - Only 1 level deep (maxdepth 1)
    # - Only directories (type d)
    # - Valid timestamp format (YYYYMMDD_HHMMSS)
    # - No processing flag file (indicates completed backup)
    log_info "Searching successful history backups..."
    local history_backups=$(
        find "$job_folder" -mindepth 1 -maxdepth 1 -type d -exec sh -c '
            flag_file="$1"
            target_dir="$2"
            dir_name=$(basename "$target_dir")
            # Validate directory name is timestamp format
            echo "$dir_name" | grep -q -E "^[0-9]{8}_[0-9]{6}$" &&
            # Validate timestamp is valid (date command parsing)
            date -jf "%Y%m%d_%H%M%S" "$dir_name" +"%Y%m%d_%H%M%S" 2>/dev/null | grep -q "^$dir_name$" &&
            # Ensure no processing flag file exists (backup completed)
            ! test -f "$target_dir/$flag_file" && echo "$target_dir"
        ' _ "$JOB_PROCESSING" {} \;
    )

    log_info "Found successful history backups: $history_backups"

    # Get last (most recent) successful backup directory (sorted alphabetically/temporally)
    LAST_BACKUP_FOLDER=$(echo "$history_backups" | sort | tail -1)
    log_info "Found newest backup as link target: $LAST_BACKUP_FOLDER"

    # Build rsync exclude list from config
    build_exclude_list

    # Build SSH command (if enabled)
    build_ssh_command

    # Define target folder for current backup (timestamped)
    TARGET_FOLDER="$TARGET_ROOT_FOLDER/$JOB_IDENTIFIER/$TIMESTAMP"
    # Create timestamped target directory
    log_info "Create backup target folder if necessary: $TARGET_FOLDER"
    mkdir -p "$TARGET_FOLDER" || {
        log_error "Failed to create target folder: $TARGET_FOLDER"
        exit 1
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# check_free_space() - FREE SPACE VALIDATION
# ----------------------------------------------------------------------------------------------------------------------
# Verifies sufficient free space exists on target filesystem:
#   1. Skips if MIN_FREE_SPACE_KB is empty/zero
#   2. Retrieves free space (in KB) of target root folder
#   3. Validates free space against minimum requirement
#
# Exits with error code 1 if:
#   - Free space retrieval fails
#   - Available space is less than required space
# ----------------------------------------------------------------------------------------------------------------------
check_free_space() {
    # Skip free space check if minimum is not set (empty or zero)
    [ -z "$MIN_FREE_SPACE_KB" ] || [ "$MIN_FREE_SPACE_KB" -eq 0 ] && {
        log_info "MIN_FREE_SPACE_KB is empty or zero, skip free space check"
        return 0
    }

    # Get free space in KB for target root folder:
    # - df -k: display free space in KB
    # - tail -n +2: skip header line
    # - awk '{print $4}': extract free space column (4th column in df output)
    local free_space_kb
    free_space_kb=$(df -k "$TARGET_ROOT_FOLDER" | tail -n +2 | awk '{print $4}' 2>/dev/null)

    # Validate free space value (must be non-empty numeric)
    [ -z "$free_space_kb" ] || ! echo "$free_space_kb" | grep -q -E '^[0-9]+$' && {
        log_error "Space check failed: Failed to get free space of '$TARGET_ROOT_FOLDER'"
        exit 1
    }

    # Check if available space is less than minimum required
    if [ "$free_space_kb" -lt "$MIN_FREE_SPACE_KB" ]; then
	log_error "Insufficient free space $free_space_kb KB on target device (Required: $MIN_FREE_SPACE_KB KB)"
        exit 1
    fi

    log_info "Check free space on target device: Available $free_space_kb KB (Required: $MIN_FREE_SPACE_KB KB)"
}

# ----------------------------------------------------------------------------------------------------------------------
# acquire_pid_lock() - Atomic PID Lock Acquisition (FreeBSD lockf Implementation)
# ----------------------------------------------------------------------------------------------------------------------
# Implements kernel-level exclusive locking via FreeBSD's native lockf utility to prevent concurrent execution
# of the same backup job. All critical filesystem operations run atomically under lock protection to eliminate
# race conditions. Subshell output is captured in-memory and forwarded to the global logging framework.
# 
# Lockf Behavior (FreeBSD /bin/sh compliant):
#   -w: Open lock file in write mode (required for exclusive locks on NFSv4/readonly filesystems)
#   -k: Retain lock file after command execution (preserves PID metadata for debugging)
#   -s: Silent mode (lock acquisition failures are indicated only by exit code)
#   -t 0: Non-blocking acquisition (fail immediately if lock is held by another process)
# ----------------------------------------------------------------------------------------------------------------------
acquire_pid_lock() {
    # Define job-specific lock file path (stored in /var/run for ephemeral state)
    PID_FILE="$PID_ROOT_FOLDER/$JOB_IDENTIFIER.pid"
    log_debug "Initiating exclusive lock acquisition on PID file: $PID_FILE"

    # Capture subshell stdout/stderr in memory (avoids temporary file bloat)
    # Preserve lockf exit code separately (critical for failure detection, as $() swallows exit codes)
    local SUB_LOGS
    local LOCKF_EXIT_CODE
    SUB_LOGS=$(
        lockf -w -k -s -t 0 "$PID_FILE" sh -c '
            set -e  # Exit immediately on any subshell command failure
            SUB_PREFIX="[subshell-PID:$$]"  # Unique prefix for subshell log attribution

            # Create PID directory with restrictive permissions if missing
            # 700 permissions: Restrict access to root user only (security hardening)
            if [ ! -d "$1" ]; then
                echo "INFO $SUB_PREFIX: Creating PID directory with 700 permissions: $1"
                mkdir -p "$1"
                chmod 700 "$1"
            fi

            # Write parent script PID to lock file (tracks active process for debugging)
            echo "INFO $SUB_PREFIX: Recording parent process PID ($3) to lock file: $2"
            echo "$3" > "$2"

            # Secure lock file with minimal permissions (prevent unauthorized modification)
            # 600 permissions: Read/write only by root (no group/other access)
            echo "INFO $SUB_PREFIX: Applying 600 permissions to lock file: $2"
            chmod 600 "$2"
        ' _ "$PID_ROOT_FOLDER" "$PID_FILE" "$$" 2>&1  # Pass parent variables as positional args (POSIX-compliant)
    )
    LOCKF_EXIT_CODE=$?

    # Forward subshell logs to global logging framework (unified log stream)
    # Read line-by-line to preserve log structure (compatible with FreeBSD /bin/sh)
    if [ -n "$SUB_LOGS" ]; then
        echo "$SUB_LOGS" | while IFS= read -r LOG_LINE; do
            log "$LOG_LINE"  # Pass full log line to underlying logging function
        done
    fi

    # Handle lock acquisition failure (critical to prevent concurrent job execution)
    if [ $LOCKF_EXIT_CODE -ne 0 ]; then
        # Identify process holding the lock (FreeBSD fuser syntax for file lock owners)
        local LOCK_HOLDING_PID=$(fuser -f "$PID_FILE" 2>/dev/null | awk '{print $1}')
        
        if [ -n "$LOCK_HOLDING_PID" ]; then
            # Capture full command line of lock-holding process for debugging
            local LOCK_HOLDING_CMD=$(ps -p "$LOCK_HOLDING_PID" -o command= -ww 2>/dev/null)
            log_error "Failed to acquire lock on $PID_FILE: Held by PID $LOCK_HOLDING_PID (Command: '$LOCK_HOLDING_CMD')"
        else
            log_error "Failed to acquire lock on $PID_FILE: Lock exists with no active holding process (possible stale lock)"
        fi
        exit 1  # Terminate to prevent concurrent execution
    fi

    # Sanity check: Verify lock file contains current script PID (defensive validation)
    local LOCK_FILE_PID=$(cat "$PID_FILE" 2>/dev/null)
    if [ "$LOCK_FILE_PID" != "$$" ]; then
        log_warning "Lock file PID mismatch: File contains $LOCK_FILE_PID (expected $$) - kernel lock remains valid"
    fi

    log_info "Successfully acquired exclusive lock on $PID_FILE (Process ID: $$)"
}

# ----------------------------------------------------------------------------------------------------------------------
# release_pid_lock() - Lock File Cleanup (FreeBSD lockf Compliance)
# ----------------------------------------------------------------------------------------------------------------------
# Cleans up the persistent lock file preserved by lockf's -k flag. The kernel-level lock is automatically released
# by lockf when the wrapped subshell completes—this function only removes the PID file to prevent false positive
# lock detection on subsequent job runs.
# 
# Safety: Only deletes the lock file if it contains the current script's PID to avoid removing locks from other
#   running processes (critical for multi-job environments).
# ----------------------------------------------------------------------------------------------------------------------
release_pid_lock() {
    # Only attempt cleanup if lock file exists
    if [ -f "$PID_FILE" ]; then
        # Read PID from lock file (suppress errors for empty/unreadable files)
        local LOCK_FILE_PID=$(cat "$PID_FILE" 2>/dev/null)
        
        # Validate lock file ownership before deletion (prevent cross-process deletion)
        if [ "$LOCK_FILE_PID" = "$$" ]; then
            if rm -f "$PID_FILE"; then
                log_info "Released lock: Removed PID file $PID_FILE (Process ID: $$)"
            else
                # Non-fatal error: Kernel lock is already released—file cleanup failure is cosmetic
                log_warning "Failed to delete lock file $PID_FILE (Process ID: $$) - kernel lock already released"
            fi
        else
            log_warning "Skipped lock file cleanup: $PID_FILE belongs to PID $LOCK_FILE_PID (current process: $$)"
        fi
    else
        log_warning "Skipped lock file cleanup: $PID_FILE does not exist (already released or never acquired)"
    fi
}

# ----------------------------------------------------------------------------------------------------------------------
# set_res_limits() - RESOURCE LIMIT CONFIGURATION
# ----------------------------------------------------------------------------------------------------------------------
# Configures resource limits for backup process:
#   1. Sets open file limit to 1024 (ulimit -n)
#   2. Lowers process priority (renice to 10) to minimize system impact
#
# All operations are silent (stderr redirected to /dev/null) to avoid unnecessary logging.
# ----------------------------------------------------------------------------------------------------------------------
set_res_limits() {
    log_info "Set resource consuming restriction..."
    # Set maximum open file descriptors to 1024 (prevents "too many open files" errors)
    ulimit -n 1024
    # Lower process priority (niceness 10) to reduce system impact (suppress stderr)
    renice -n 10 $$ 2>/dev/null
}

# ----------------------------------------------------------------------------------------------------------------------
# perform_job() - CORE BACKUP EXECUTION
# ----------------------------------------------------------------------------------------------------------------------
# Executes the core rsync backup operation:
#   1. Validates target folder is empty (prevents overwriting existing data)
#   2. Creates processing flag file (indicates backup in progress)
#   3. Builds rsync command with:
#      - Archive mode (preserves permissions/ownership)
#      - Incremental backup (--link-dest) if last backup exists
#      - Exclude list (from build_exclude_list())
#      - SSH command (from build_ssh_command())
#      - Logging to job log file
#   4. Executes rsync command and checks exit code
#   5. Cleans up target folder if rsync fails
#   6. Removes processing flag file on success
#
# Exits with error code 1 if rsync fails or flag file operations fail.
# ----------------------------------------------------------------------------------------------------------------------
perform_job() {
    log_info "Checking target folder..."
    # Check if target folder is not empty (prevents overwriting existing data)
    if [ -n "$(ls -A "$TARGET_FOLDER" 2>/dev/null)" ]; then
        log_error "Target directory is not empty: $TARGET_FOLDER"
        exit 1
    fi

    log_info "Creating processing flag file..."
    # Create processing flag file (indicates backup is in progress)
    local job_flag="$TARGET_FOLDER/$JOB_PROCESSING"
    touch "$job_flag" || {
        log_error "Failed to create processing flag: $job_flag"
        exit 1
    }

    # Build base rsync command with archive options:
    # -a: archive mode (recursive, preserve permissions, ownership, timestamps, etc.)
    # -A: preserve ACLs
    # -X: preserve extended attributes
    # -H: preserve hard links
    # --numeric-ids: use numeric UID/GID instead of names (avoids lookup issues)
    # --delete: delete extraneous files from target (mirror source)
    # --quiet: reduce output verbosity
    # --sparse: handle sparse files efficiently
    local rsync_cmd="rsync -aAXH --numeric-ids --delete --quiet --sparse"

    # Add incremental backup support (--link-dest) if last backup exists
    [ -n "$LAST_BACKUP_FOLDER" ] && {
        rsync_cmd="$rsync_cmd --link-dest='$LAST_BACKUP_FOLDER'"
        log_info "Enabled incremental backup via --link-dest: '$LAST_BACKUP_FOLDER'"
    }

    # Add exclude list to rsync command if defined
    [ -n "$EXCLUDE_LIST" ] && rsync_cmd="$rsync_cmd $EXCLUDE_LIST"
    # Add SSH command to rsync command if defined (remote backups)
    [ -n "$SSH_COMMAND" ] && rsync_cmd="$rsync_cmd -e '$SSH_COMMAND'"
    # Add log file to rsync command (rsync logs to same file as script)
    rsync_cmd="$rsync_cmd --log-file=$LOG_FILE"
    # Add source and target to rsync command (trailing slash on target for correct rsync behavior)
    rsync_cmd="$rsync_cmd '$SOURCE_FOLDER' '$TARGET_FOLDER/'"

    # Record start time (Unix epoch seconds) for duration calculation
    START_TIME=$(date +%s)

    log_info "Rsync backup command: $rsynv_cmd"
    log_info "Starting rsync backup..."
    # Execute rsync command (redirect stdout to null, stderr to stdout for error capture)
    if eval "$rsync_cmd" 2>&1 >/dev/null; then
        log_info "Rsync backup completed successfully"
    else
        # Capture rsync exit code for debugging
        local rsync_exit_code=$?
        log_error "Rsync backup failed, exit code: $rsync_exit_code"
        # Clean up failed backup directory
        rm -rf "$TARGET_FOLDER" 2>/dev/null
        exit 1
    fi

    # Remove processing flag file (indicates backup completed successfully)
    rm -f "$job_flag" || {
        log_error "Failed to remove processing flag: $job_flag"
        exit 1
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# show_statistics() - BACKUP STATISTICS REPORTING
# ----------------------------------------------------------------------------------------------------------------------
# Calculates and logs backup statistics:
#   1. Backup size (human-readable format via du -sh)
#   2. Backup duration (seconds from START_TIME to current time)
#
# Provides visibility into backup performance and storage usage.
# ----------------------------------------------------------------------------------------------------------------------
show_statistics() {
    # Calculate backup size (human-readable format: KB, MB, GB, etc.)
    local size=$(du -sh "$TARGET_FOLDER" 2>/dev/null | cut -f1)
    # Calculate backup duration (current time - start time in seconds)
    local duration=$(( $(date +%s) - $START_TIME ))
    # Log backup statistics
    log_info "Backup completed: Size=$size, Duration=${duration}s"
}

# ----------------------------------------------------------------------------------------------------------------------
# clean_failure_backups() - FAILED BACKUP CLEANUP
# ----------------------------------------------------------------------------------------------------------------------
# Removes failed/incomplete backup directories when ENABLE_AUTO_CLEAN=true:
#   1. Skips if auto-clean is disabled
#   2. Identifies directories with processing flag file (indicates failed/incomplete backup)
#   3. Removes identified directories (logs each removal)
# ----------------------------------------------------------------------------------------------------------------------
clean_failure_backups() {
    # Skip if auto-clean is disabled in config
    [ "$ENABLE_AUTO_CLEAN" != "true" ] && {
        log_info "Auto clean disabled, skipping cleanup failure backups"
        return 0
    }

    # Define job-specific target directory
    local job_folder="$TARGET_ROOT_FOLDER/$JOB_IDENTIFIER"

    # Find failed/incomplete backup directories:
    # - Valid timestamp format
    # - Contains processing flag file (indicates incomplete/failed backup)
    local failure_backup_dirs=$(find "$job_folder" -mindepth 1 -maxdepth 1 -type d -exec sh -c '
        flag_file="$1"
        target_dir="$2"
        dir_name=$(basename "$target_dir")
        # Validate timestamp format
        date -jf "%Y%m%d_%H%M%S" "$dir_name" +"%Y%m%d_%H%M%S" 2>/dev/null | grep -q "^$dir_name$" &&
        # Check for processing flag file (incomplete backup)
        test -f "$target_dir/$flag_file" && echo "$dir_name"
    ' _ "$JOB_PROCESSING" {} \; | sort)
    
    # Remove each failed backup directory
    [ -n "$failure_backup_dirs" ] && echo "$failure_backup_dirs" | while read -r failure_backup; do
        [ -n "$failure_backup" ] && {
            log_info "Remove failure backup directory $job_folder/$failure_backup"
            rm -rf "$job_folder/$failure_backup"
        }
    done

    return 0
}

# ----------------------------------------------------------------------------------------------------------------------
# clean_old_backups() - OLD BACKUP CLEANUP
# ----------------------------------------------------------------------------------------------------------------------
# Rotates old backup directories when ENABLE_AUTO_CLEAN=true and RETAIN_COUNT>0:
#   1. Skips if auto-clean is disabled or RETAIN_COUNT is empty/zero
#   2. Identifies all valid backup directories (timestamp-named, no processing flag)
#   3. Calculates number of backups exceeding RETAIN_COUNT
#   4. Removes oldest backups to maintain RETAIN_COUNT limit
# ----------------------------------------------------------------------------------------------------------------------
clean_old_backups() {
    # Skip if auto-clean is disabled or retain count is zero/empty
    [ "$ENABLE_AUTO_CLEAN" = "true" ] && [ -n "$RETAIN_COUNT" ] && [ "$RETAIN_COUNT" -ne 0 ] || {
        log_info "Auto clean disabled, or RETAIN_COUNT is empty or zero, skip clean up old backups"
        return 0
    }

    # Define job-specific target directory
    local job_folder="$TARGET_ROOT_FOLDER/$JOB_IDENTIFIER"
    # Get retain count from config/command line
    local retain="$RETAIN_COUNT"
    
    # Find all valid backup directories (timestamp format, no processing flag)
    local backup_dirs=$(find "$job_folder" -mindepth 1 -maxdepth 1 -type d -exec sh -c '
        target_dir="$1"
        dir_name=$(basename "$target_dir")
        # Validate timestamp format
        date -jf "%Y%m%d_%H%M%S" "$dir_name" +"%Y%m%d_%H%M%S" 2>/dev/null | grep -q "^$dir_name$" && echo "$dir_name"
    ' _ {} \; | sort)
    
    # Count number of valid backup directories
    local count=$(echo "$backup_dirs" | wc -l)
    
    # Remove oldest backups if count exceeds retain limit
    [ "$count" -gt "$retain" ] && echo "$backup_dirs" | head -n $((count - retain)) | \
    while read -r old_backup; do
        [ -n "$old_backup" ] && {
            log_info "Remove old backup directory $job_folder/$old_backup"
            rm -rf "$job_folder/$old_backup"
        }
    done

    return 0
}

# ----------------------------------------------------------------------------------------------------------------------
# clean_old_logs() - OLD LOG FILE CLEANUP
# ----------------------------------------------------------------------------------------------------------------------
# Rotates old log files when ENABLE_AUTO_CLEAN=true and RETAIN_COUNT>0:
#   1. Skips if auto-clean is disabled or RETAIN_COUNT is empty/zero
#   2. Identifies all valid log files (timestamp-named .log files)
#   3. Calculates number of logs exceeding RETAIN_COUNT
#   4. Removes oldest logs to maintain RETAIN_COUNT limit
# ----------------------------------------------------------------------------------------------------------------------
clean_old_logs() {
    # Skip if auto-clean is disabled or retain count is zero/empty
    [ "$ENABLE_AUTO_CLEAN" = "true" ] && [ -n "$RETAIN_COUNT" ] && [ "$RETAIN_COUNT" -ne 0 ] || {
        log_info "Auto clean disabled, or RETAIN_COUNT is empty or zero, skip clean up logs"
        return 0
    }

    # Define job-specific log directory
    local log_folder="$LOG_ROOT_FOLDER/$JOB_IDENTIFIER"
    # Get retain count from config/command line
    local retain="$RETAIN_COUNT"
    # Find all valid log files (timestamp format with .log extension)
    local files=$(ls "$log_folder" 2>/dev/null | grep -E '^[0-9]{8}_[0-9]{6}\.log$' | sort)
    # Count number of valid log files
    local count=$(echo "$files" | wc -l)

    # Remove oldest logs if count exceeds retain limit
    [ "$count" -gt "$retain" ] && echo "$files" | head -n $((count - retain)) | \
    while read -r old_log; do
        [ -n "$old_log" ] && {
            log_info "Remove old log file $log_folder/$old_log"
            rm -f "$log_folder/$old_log"
        }
    done

    return 0
}

# ----------------------------------------------------------------------------------------------------------------------
# cleanup() - EMERGENCY CLEANUP HANDLER
# ----------------------------------------------------------------------------------------------------------------------
# Emergency cleanup function triggered by EXIT/INT/TERM/HUP signals:
#   1. Logs unexpected termination (if exit code != 0)
#   2. Cleans up incomplete backup directories (has processing flag)
#   3. Releases PID lock (via release_pid_lock())
#   4. Unmounts backup device (if ENABLE_AUTOMOUNT=true)
#   5. Logs final exit code and exits
#
# Ensures system is left in consistent state even if script is interrupted.
# ----------------------------------------------------------------------------------------------------------------------
cleanup() {
    # Capture exit code of the script/triggering command
    local exit_code=$?

    # Log unexpected termination (non-zero exit code)
    if [ "$exit_code" -ne 0 ]; then
        log_info "Program terminated unexpectedly, code: $exit_code"
    fi

    # Clean up incomplete backup directory (has processing flag file)
    if [ -n "$TARGET_FOLDER" ] && [ -d "$TARGET_FOLDER" ] && [ -f "$TARGET_FOLDER/$JOB_PROCESSING" ]; then
        log_info "Cleaning uncompleted backup..."
        rm -rf "$TARGET_FOLDER" 2>/dev/null
    fi

    # Release PID lock (prevents stale locks)
    release_pid_lock

    # Unmount backup device if automount is enabled
    [ "$ENABLE_AUTOMOUNT" = "true" ] && umount_device

    # Log final exit code
    log_info "Program exit with code $exit_code"
    exit $exit_code
}

# ----------------------------------------------------------------------------------------------------------------------
# main() - SCRIPT MAIN EXECUTION FLOW
# ----------------------------------------------------------------------------------------------------------------------
# Main entry point for script execution - orchestrates all backup operations:
#   1. Validates script is run as root (UID 0)
#   2. Parses command line arguments (parse_arguments())
#   3. Validates configuration (check_config())
#   4. Initializes timestamp and log file
#   5. Sets up signal traps for emergency cleanup
#   6. Mounts backup device (if enabled)
#   7. Prepares backup job environment
#   8. Validates free space on target
#   9. Acquires PID lock (prevents concurrent execution)
#   10. Sets resource limits
#   11. Executes core backup operation
#   12. Reports backup statistics
#   13. Cleans up failed/old backups and old logs
#   14. Exits with success code (0)
#
# All critical errors trigger exit with non-zero code (handled by cleanup() via trap).
# ----------------------------------------------------------------------------------------------------------------------
main() {
    # Enforce root execution (required for mount/unmount, system file access)
    if [ "$(id -u)" -ne 0 ]; then
        log_error "This script must be run as root (UID 0)"
        exit 1
    fi

    # Parse and validate command line arguments
    parse_arguments "$@"

    # Validate configuration parameters
    check_config

    # Initialize timestamp for backup/log naming
    init_timestamp

    # Initialize log file for current job
    init_log

    # Enable exit on error (strict mode)
    set -e
    # Set up signal traps for emergency cleanup (EXIT, INT, TERM, HUP)
    trap 'cleanup' EXIT INT TERM HUP

    # Mount backup device (if automount enabled)
    mount_device

    # Prepare backup job environment (folders, exclude list, SSH command)
    prepare_job

    # Check free space on target filesystem
    check_free_space

    # Acquire PID lock (prevent concurrent execution)
    acquire_pid_lock

    # Set resource limits (open files, process priority)
    set_res_limits

    # Execute core rsync backup
    perform_job

    # Log backup statistics (size, duration)
    show_statistics

    # Clean up failed backups (if auto-clean enabled)
    clean_failure_backups
    
    # Clean up old backups (if auto-clean enabled)
    clean_old_backups

    # Clean up old log files (if auto-clean enabled)
    clean_old_logs

    # Exit with success code
    exit 0
}

# Execute main function with all command line arguments
main "$@"

# TODO list
# --verify: verify backup integrity
# --dry-run
# send mail on fail
# exclude dir inline comments and blank line
# security check for conf file
