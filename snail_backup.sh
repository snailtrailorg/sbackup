#!/bin/sh
########################################################################################################################
########################################################################################################################
# GUIDE TO RESTORE BACKUPS
# 1. Preparations
#    - Define BACKUP_DIR (path to rsync backup) and RESTORE_DIR (target restore path)
#    - Mount target filesystem if necessary (e.g., mount /dev/ada0p2 $RESTORE_DIR)
#
# 2. Backup Consistency Check
#    - Dry-run to verify backup structure: rsync -n --delete --numeric-ids $BACKUP_DIR/ $RESTORE_DIR/
#
# 3. Core Restoration Command
#    - Restore with archive mode (preserve permissions/ownership):
#      rsync -av --delete --numeric-ids $BACKUP_DIR/ $RESTORE_DIR/
#
# 4. Post-Restoration Verification
#    - Compare backup and restored files: diff -r $BACKUP_DIR/ $RESTORE_DIR/
#    - Check file ownership/permissions: ls -l $RESTORE_DIR
#
# 5. Chroot Validation (for system-level restore)
#    - Enter restored environment: chroot $RESTORE_DIR (exit with 'exit')
#    - Verify critical system files (e.g., ls /etc, cat /etc/passwd)
#
# 6. Restore to Original Source Directory
#    - Unmount the restore mount point (e.g., umount $RESTORE_DIR)
#    - Reboot to apply restoration to original system directory
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

# Log function - simplified without padding
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date +"%Y-%m-%d %H:%M:%S")

    local log_entry="[${timestamp}] [${level}] $message"

    # Write to log file
    if [ -n "$LOG_FILE" ]; then
        echo "$log_entry" >> "$LOG_FILE"
    fi

    # Output to appropriate stream based on level
    case "$level" in
        WARNING|ERROR)
            echo "$log_entry" >&2
            ;;
        *)
            echo "$log_entry"
            ;;
    esac
}

log_debug() {
    log "DEBUG" "$@"
}

log_info() {
    log "INFO" "$@"
}

log_warning() {
    log "WARNING" "$@"
}

log_error() {
    log "ERROR" "$@"
}

check_commands() {
    local missing_commands=""

    for cmd in awk basename cat chmod cut date df find grep head ls mkdir mount nice ps renice rm rsync sed sort ssh stat tail touch tr umount wc; do
        if command -v "$cmd" >/dev/null 2>&1; then
            log_info "check command $cmd ... OK"
        else
            log_warning "check command $cmd ... FAILED"
            missing_commands="$missing_commands $cmd"
        fi
    done

    if [ -n "$missing_commands" ]; then
        log_warning "Missing commands:$missing_commands"
    fi
}

usage() {
    local script_name=$(basename "$0")
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

find_config_file() {
    local config_file="$1"
    local found_file=""

    if [ "${config_file#/}" != "$config_file" ]; then
        if [ -f "$config_file" ]; then
            echo "$config_file"
            return 0
        else
            return 1
        fi
    fi

    local folder
    for folder in $CONFIG_FILE_FOLDERS; do
        local potential_file="$folder/$config_file"
        if [ -f "$potential_file" ]; then
            found_file="$potential_file"
            break
        fi
    done

    if [ -n "$found_file" ]; then
        echo "$found_file"
        return 0
    else
        return 1
    fi
}

# Parse commandline arguments
parse_arguments() {
    [ $# -eq 0 ] && {
        usage
        exit 0
    }

    local config_file=""

    while [ $# -gt 0 ]; do
        case "$1" in
        -h|--help)
            usage
            exit 0
            ;;

        -s|--self-check)
            check_commands
            exit 0
            ;;

        -c|--config-file)
            config_file=$(find_config_file "$2")
            if [ $? -ne 0 ]; then
                log_error "Cannot find config file: $2"
                usage
                exit 1
            fi

            if [ ! -r "$config_file" ]; then
                log_error "Config file is not readable: $config_file"
                exit 1
            fi

            local key_perms=$(stat -f "%Lp" "$config_file" 2>/dev/null)
            [ "$key_perms" -eq 400 ] || [ "$key_perms" -eq 600 ] || {
                log_error "Config file permissions too open: $config_file (0$key_perms)"
                exit 1
            }

            . "$config_file"
            if [ $? -ne 0 ]; then
                log_error "Parse config file error: $config_file"
                exit 1
            fi

            echo "Loaded config file $config_file"
            shift 2
            ;;

        -j|--job-identifier)
            if ! echo "$2" | grep -q -E '^[a-zA-Z0-9_]+$'; then
                log_error "Illegal job identifier: $2, only a-z, A-Z, 0-9 and '_' are allowed"
                exit 1
            fi

            JOB_IDENTIFIER="$2"
            echo "Set job identifier to $JOB_IDENTIFIER"
            shift 2
            ;;

        -a|--auto-clean)
            [ "$2" != "true" ] && [ "$2" != "false" ] && {
                log_error "Invalid parameter value: '$2' (must be 'true' or 'false')"
                exit 1
            }

            ENABLE_AUTO_CLEAN="$2"
            shift 2
            ;;

        -r|--retain-count)
            if ! echo "$2" | grep -q -E '^[1-9][0-9]*$'; then
                log_error "Retain count must be a positive number"
                exit 1
            fi

            RETAIN_COUNT="$2"
            shift 2
            ;;

        *)
            log_error "Unexpected argument: $1"
            usage
            exit 1
            ;;
        esac
    done

    if [ -z "$config_file" ]; then
        log_error "Config file must be specified with -c or --config-file"
        usage
        exit 1
    fi
}

check_config() {
    [ -n "$ENABLE_AUTOMOUNT" ] && [ "$ENABLE_AUTOMOUNT" != "true" ] && [ "$ENABLE_AUTOMOUNT" != "false" ] && {
        log_error "ENABLE_AUTOMOUNT must be 'true' or 'false'"
        exit 1
    }

    [ "$ENABLE_AUTOMOUNT" = "true" ] && [ -z "$MOUNT_DEVICE" ] && {
        log_error "MOUNT_DEVICE cannot be empty while ENABLE_AUTOMOUNT is true"
        exit 1
    }

    [ "$ENABLE_AUTOMOUNT" = "true" ] && [ -z "$MOUNT_POINT" ] && {
        log_error "MOUNT_POINT cannot be empty while ENABLE_AUTOMOUNT is true"
        exit 1
    }

    [ -n "$ENABLE_SSH" ] && [ "$ENABLE_SSH" != "true" ] && [ "$ENABLE_SSH" != "false" ] && {
        log_error "ENABLE_SSH must be 'true' or 'false'"
        exit 1
    }

    [ "$ENABLE_SSH" = "true" ] && {
        [ -n "$SSH_PORT" ] && ! (echo "$SSH_PORT" | grep -q -E '^[0-9]+$' && [ "$SSH_PORT" -ge 1 ] && [ "$SSH_PORT" -le 65535 ]) && {
            log_error "SSH_PORT must be an integer between 1 and 65535 when ENABLE_SSH is true"
            exit 1
        }

        [ -n "$SSH_KEY_FILE" ] && {
            [ ! -f "$SSH_KEY_FILE" ] || [ ! -r "$SSH_KEY_FILE" ] && {
                log_error "SSH_KEY_FILE does not exist or is not readable"
                exit 1
            }

            local key_perms=$(stat -f "%Lp" "$SSH_KEY_FILE" 2>/dev/null)
            [ "$key_perms" -ne 400 ] && [ "$key_perms" -ne 600 ] && {
                log_error "SSH key file permissions $key_perms is too open, should be 600 or 400"
                exit 1
            }
        }
    }

    [ -z "$SOURCE_FOLDER" ] && {
        log_error "SOURCE_FOLDER cannot be empty"
        exit 1
    }

    [ -z "$TARGET_ROOT_FOLDER" ] && {
        log_error "TARGET_ROOT_FOLDER cannot be empty"
        exit 1
    }

    [ -n "$MIN_FREE_SPACE_KB" ] && ! echo "$MIN_FREE_SPACE_KB" | grep -q -E '^[0-9]+$' && {
        log_error "MIN_FREE_SPACE_KB must be empty or a non-negative integer"
        exit 1
    }

    [ -z "$JOB_IDENTIFIER" ] && {
        log_error "Job identifier must be set, see JOB_IDENTIFIER in config file and --job-identifier command line option"
        exit 1
    }

    echo "$JOB_IDENTIFIER" | grep -q -E '^[a-zA-Z0-9_]+$' || {
        log_error "JOB_IDENTIFIER can only contain letters, numbers and underscores"
        exit 1
    }

    [ -n "$ENABLE_AUTO_CLEAN" ] && [ "$ENABLE_AUTO_CLEAN" != "true" ] && [ "$ENABLE_AUTO_CLEAN" != "false" ] && {
        log_error "ENABLE_AUTO_CLEAN must be 'true' or 'false'"
        exit 1
    }
}

init_timestamp() {
    TIMESTAMP=$(date +%Y%m%d_%H%M%S) || {
        log_error "Failed to get timestamp"
        exit 1
    }
}

init_log() {
    mkdir -p "$LOG_ROOT_FOLDER/$JOB_IDENTIFIER" || {
        log_error "Failed to create log folder: $LOG_ROOT_FOLDER/$JOB_IDENTIFIER"
        exit 1
    }

    chmod 700 "$LOG_ROOT_FOLDER/$JOB_IDENTIFIER" || {
        log_error "Failed to set permissions to 700 on log folder: $LOG_ROOT_FOLDER/$JOB_IDENTIFIER"
        exit 1
    }

    LOG_FILE="$LOG_ROOT_FOLDER/$JOB_IDENTIFIER/${TIMESTAMP}.log"
    touch "$LOG_FILE" || {
        log_error "Failed to touch log file: $LOG_FILE"
        exit 1
    }
    chmod 600 "$LOG_FILE" || {
        log_error "Failed to set permissions on log file: $LOG_FILE"
        exit 1
    }
}

mount_device() {
    [ "$ENABLE_AUTOMOUNT" != "true" ] && {
        log_info "Automount is disabled (ENABLE_AUTOMOUNT=false), skip mounting"
        return 0
    }

    [ ! -d "$MOUNT_POINT" ] && {
        log_info "Mount point $MOUNT_POINT does not exist, creating..."
        mkdir -p "$MOUNT_POINT" || {
            log_error "Failed to create mount point $MOUNT_POINT"
            exit 1
        }
    }

    if mount | grep -q "^$MOUNT_DEVICE on $MOUNT_POINT "; then
        log_info "Device $MOUNT_DEVICE is already mounted on $MOUNT_POINT"
        return 0
    fi

    local mount_cmd
    [ -n "$MOUNT_FS_TYPE" ] && mount_cmd="mount -t $MOUNT_FS_TYPE $MOUNT_DEVICE $MOUNT_POINT" ||
        mount_cmd="mount $MOUNT_DEVICE $MOUNT_POINT"

    log_info "Executing mount command: $mount_cmd"

    eval "$mount_cmd" 2>/dev/null && {
        log_info "Successfully mounted $MOUNT_DEVICE to $MOUNT_POINT"
        return 0
    } || {
        log_error "Failed to mount $MOUNT_DEVICE to $MOUNT_POINT"
        exit 1
    }
}

umount_device() {
    [ "$ENABLE_AUTOMOUNT" != "true" ] && {
        log_info "Automount is disabled (ENABLE_AUTOMOUNT=false), skip unmounting"
        return 0
    }

    ! mount | grep -qF "$MOUNT_POINT" && {
        log_info "Mount point $MOUNT_POINT is not mounted, skip unmounting"
        return 0
    }

    local umount_cmd="umount $MOUNT_POINT"
    log_info "Executing unmount command: $umount_cmd"

    eval "$umount_cmd" 2>/dev/null && {
        log_info "Successfully unmounted $MOUNT_POINT"
        return 0
    } || {
        log_error "Failed to unmount $MOUNT_POINT"
        exit 1
    }
}

build_exclude_list() {
    [ -z "$EXCLUDE_DIRS" ] && return 0

    set -f

    while read -r line; do
        line_processed=$(printf '%s' "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
        [ -z "$line_processed" ] && continue

        if [ -z "$EXCLUDE_LIST" ]; then
            EXCLUDE_LIST="--exclude=\"$line_processed\""
        else
            EXCLUDE_LIST="$EXCLUDE_LIST --exclude=\"$line_processed\""
        fi
    done <<- EOF
		$EXCLUDE_DIRS
	EOF

    set +f
}

__build_exclude_list() {
    [ -z "$EXCLUDE_DIRS" ] && return 0

    set -f

    printf '%s\n' "$EXCLUDE_DIRS" | while read -r line; do
        line_processed=$(echo "$line" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
        [ -z "$line_processed" ] && continue
        EXCLUDE_LIST="$EXCLUDE_LIST --exclude=\"$line_processed\""
    done

    set +f

    EXCLUDE_LIST=$(echo "$EXCLUDE_LIST" | sed -e 's/^[[:space:]]*//')
}

_build_exclude_list() {
    [ -z "$EXCLUDE_DIRS" ] && return 0

    IFS='
'
    for line in $EXCLUDE_DIRS; do
        unset IFS

        line_processed=$(echo "$line" | sed -e 's/[[:space:]]*$//' -e 's/^[[:space:]]*//')
        [ -z "$line_processed" ] && continue

        EXCLUDE_LIST="$EXCLUDE_LIST --exclude=$line_processed"

        IFS='
'
    done
    unset IFS
}

build_ssh_command() {
    if [ "$ENABLE_SSH" = "false" ]; then
        log_info "SSH is disabled (ENABLE_SSH=false)"
        return 0
    fi

    log_info "SSH is enabled (ENABLE_SSH=true), building SSH command..."
    SSH_COMMAND="ssh"

    if [ -n "$SSH_PORT" ]; then
        SSH_COMMAND="$SSH_COMMAND -p $SSH_PORT"
    fi

    if [ -n "$SSH_KEY_FILE" ]; then
        SSH_COMMAND="$SSH_COMMAND -i $SSH_KEY_FILE"
    fi
}

prepare_job() {
    local job_folder="$TARGET_ROOT_FOLDER/$JOB_IDENTIFIER"
    mkdir -p "$job_folder" || {
        log_error "Failed to create target job folder: $job_folder"
        exit 1
    }

    local history_backups=$(
        find "$job_folder" -mindepth 1 -maxdepth 1 -type d -exec sh -c '
            flag_file="$1"
            target_dir="$2"
            dir_name=$(basename "$target_dir")
            echo "$dir_name" | grep -q -E "^[0-9]{8}_[0-9]{6}$" &&
            date -jf "%Y%m%d_%H%M%S" "$dir_name" +"%Y%m%d_%H%M%S" 2>/dev/null | grep -q "^$dir_name$" &&
            ! test -f "$target_dir/$flag_file" && echo "$target_dir"
        ' _ "$JOB_PROCESSING" {} \;
    )

    log_info "Found successful history backups: $history_backups"

    LAST_BACKUP_FOLDER=$(echo "$history_backups" | sort | tail -1)

    build_exclude_list

    build_ssh_command

    TARGET_FOLDER="$TARGET_ROOT_FOLDER/$JOB_IDENTIFIER/$TIMESTAMP"
    mkdir -p "$TARGET_FOLDER" || {
        log_error "Failed to create target folder: $TARGET_FOLDER"
        exit 1
    }
}

check_free_space() {
    [ -z "$MIN_FREE_SPACE_KB" ] || [ "$MIN_FREE_SPACE_KB" -eq 0 ] && {
        log_info "MIN_FREE_SPACE_KB is empty or zero, skip free space check"
        return 0
    }

    local free_space_kb
    free_space_kb=$(df -k "$TARGET_ROOT_FOLDER" | tail -n +2 | awk '{print $4}' 2>/dev/null)

    [ -z "$free_space_kb" ] || ! echo "$free_space_kb" | grep -q -E '^[0-9]+$' && {
        log_error "Space check failed: Failed to get free space of '$TARGET_ROOT_FOLDER'"
        exit 1
    }

    if [ "$free_space_kb" -lt "$MIN_FREE_SPACE_KB" ]; then
        log_error "Insufficient free space $free_space_kb KB where needs $MIN_FREE_SPACE_KB KB on $TARGET_ROOT_FOLDER"
        exit 1
    fi

    log_info "Free space check passed: Available $free_space_kb KB (Required: $MIN_FREE_SPACE_KB KB)"
}

acquire_pid_lock() {
    if [ ! -d "$PID_ROOT_FOLDER" ]; then
        mkdir -p "$PID_ROOT_FOLDER" || {
            log_error "Failed to create PID folder: $PID_ROOT_FOLDER"
            exit 1
        }

        chmod 700 "$PID_ROOT_FOLDER" || {
            log_error "Failed to set permissions to 700 on PID folder: $PID_ROOT_FOLDER"
            exit 1
        }
    fi

    PID_FILE="$PID_ROOT_FOLDER/$JOB_IDENTIFIER.pid"

    if [ -f "$PID_FILE" ]; then
        if [ -r "$PID_FILE" ]; then
            if lock_pid=$(cat "$PID_FILE" 2>/dev/null) && [ -n "$lock_pid" ]; then
                if ps -p "$lock_pid" >/dev/null 2>&1; then
                    local cmd_line=$(ps -p "$lock_pid" -o command= -ww 2>/dev/null)
                    local process_name=$(basename "$0")
                    if echo "$cmd_line" | grep -qF "$process_name"; then
                        if echo "$cmd_line" | grep -qF "$JOB_IDENTIFIER"; then
                            log_error "Another process of job '$JOB_IDENTIFIER' is running: PID=$lock_pid; CMD=$cmd_line"
                            exit 1
                        else
                            log_warning "Another JOB matching the historical PID (from $PID_FILE) of this JOB is running: PID=$lock_pid; CMD=$cmd_line"
                        fi
                    else
                        log_warning "Another process matching the historical PID (from $PID_FILE) is running: PID=$lock_pid; CMD=$cmd_line"
                    fi
                else
                    log_warning "Found stale PID $lock_pid in $PID_FILE, no running process, ignore it"
                fi
            else
                log_warning "Empty PID file $PID_FILE exist, ignore it"
            fi
        else
            log_error "PID file $PID_FILE exist but cannot readable"
            exit 1
        fi
    fi

    echo $$ > "$PID_FILE" || {
        log_error "Failed to write PID file $PID_FILE"
        exit 1
    }

    chmod 600 "$PID_FILE" || {
        log_error "Failed to set permissions to 600 on PID file $PID_FILE"
        exit 1
    }

    log_info "Acquired PID lock $PID_FILE with process id $$"
}

release_pid_lock() {
    if [ -f "$PID_FILE" ]; then
        if [ "$(cat "$PID_FILE")" = "$$" ]; then
            rm -f "$PID_FILE"
            if [ "$?" -eq 0 ]; then
                log_info "Released pid lock $PID_FILE"
            else
                log_warning "Failed to remove PID file $PID_FILE"
            fi
        else
            log_warning "PID file $PID_FILE has been modified to $(head -n 1 "$PID_FILE")"
        fi
    else
        log_warning "PID file doesn't exist"
    fi
}

set_res_limits() {
    ulimit -n 1024
    renice -n 10 $$ 2>/dev/null
}

perform_job() {
    if [ -n "$(ls -A "$TARGET_FOLDER" 2>/dev/null)" ]; then
        log_error "Target directory is not empty: $TARGET_FOLDER"
        exit 1
    fi

    local job_flag="$TARGET_FOLDER/$JOB_PROCESSING"
    touch "$job_flag" || {
        log_error "Failed to create processing flag: $job_flag"
        exit 1
    }

    local rsync_cmd="rsync -aAXH --numeric-ids --delete --quiet --sparse"

    [ -n "$LAST_BACKUP_FOLDER" ] && {
        rsync_cmd="$rsync_cmd --link-dest='$LAST_BACKUP_FOLDER'"
        log_info "Enabled incremental backup via --link-dest: '$LAST_BACKUP_FOLDER'"
    }

    [ -n "$EXCLUDE_LIST" ] && rsync_cmd="$rsync_cmd $EXCLUDE_LIST"
    [ -n "$SSH_COMMAND" ] && rsync_cmd="$rsync_cmd -e '$SSH_COMMAND'"
    rsync_cmd="$rsync_cmd --log-file=$LOG_FILE"
    rsync_cmd="$rsync_cmd '$SOURCE_FOLDER' '$TARGET_FOLDER/'"

    START_TIME=$(date +%s)

    log_info "Starting rsync backup..."
    if eval "$rsync_cmd" 2>&1 >/dev/null; then
        log_info "Rsync backup completed successfully"
    else
        local rsync_exit_code=$?
        log_error "Rsync backup failed, exit code: $rsync_exit_code"
        rm -rf "$TARGET_FOLDER" 2>/dev/null
        exit 1
    fi

    rm -f "$job_flag" || {
        log_error "Failed to remove processing flag: $job_flag"
        exit 1
    }
}

show_statistics() {
    local size=$(du -sh "$TARGET_FOLDER" 2>/dev/null | cut -f1)
    local duration=$(( $(date +%s) - $START_TIME ))
    log_info "Backup completed: Size=$size, Duration=${duration}s"
}

clean_failure_backups() {
    [ "$ENABLE_AUTO_CLEAN" != "true" ] && {
        log_info "Auto clean disabled, skipping cleanup failure backups"
        return 0
    }

    local job_folder="$TARGET_ROOT_FOLDER/$JOB_IDENTIFIER"

    local failure_backup_dirs=$(find "$job_folder" -mindepth 1 -maxdepth 1 -type d -exec sh -c '
        flag_file="$1"
        target_dir="$2"
        dir_name=$(basename "$target_dir")
        date -jf "%Y%m%d_%H%M%S" "$dir_name" +"%Y%m%d_%H%M%S" 2>/dev/null | grep -q "^$dir_name$" &&
        test -f "$target_dir/$flag_file" && echo "$dir_name"
    ' _ "$JOB_PROCESSING" {} \; | sort)
    
    [ -n "$failure_backup_dirs" ] && echo "$failure_backup_dirs" | while read -r failure_backup; do
        [ -n "$failure_backup" ] && {
            log_info "Remove failure backup directory $job_folder/$failure_backup"
            rm -rf "$job_folder/$failure_backup"
        }
    done

    return 0
}

clean_old_backups() {
    [ "$ENABLE_AUTO_CLEAN" = "true" ] && [ -n "$RETAIN_COUNT" ] && [ "$RETAIN_COUNT" -ne 0 ] || {
        log_info "Auto clean disabled, or RETAIN_COUNT is empty or zero, skip clean up old backups"
        return 0
    }

    local job_folder="$TARGET_ROOT_FOLDER/$JOB_IDENTIFIER"
    local retain="$RETAIN_COUNT"
    
    local backup_dirs=$(find "$job_folder" -mindepth 1 -maxdepth 1 -type d -exec sh -c '
        target_dir="$1"
        dir_name=$(basename "$target_dir")
        date -jf "%Y%m%d_%H%M%S" "$dir_name" +"%Y%m%d_%H%M%S" 2>/dev/null | grep -q "^$dir_name$" && echo "$dir_name"
    ' _ {} \; | sort)
    
    local count=$(echo "$backup_dirs" | wc -l)
    
    [ "$count" -gt "$retain" ] && echo "$backup_dirs" | head -n $((count - retain)) | \
    while read -r old_backup; do
        [ -n "$old_backup" ] && {
            log_info "Remove old backup directory $job_folder/$old_backup"
            rm -rf "$job_folder/$old_backup"
        }
    done

    return 0
}

clean_old_logs() {
    [ "$ENABLE_AUTO_CLEAN" = "true" ] && [ -n "$RETAIN_COUNT" ] && [ "$RETAIN_COUNT" -ne 0 ] || {
        log_info "Auto clean disabled, or RETAIN_COUNT is empty or zero, skip clean up logs"
        return 0
    }

    local log_folder="$LOG_ROOT_FOLDER/$JOB_IDENTIFIER"
    local retain="$RETAIN_COUNT"
    local files=$(ls "$log_folder" 2>/dev/null | grep -E '^[0-9]{8}_[0-9]{6}\.log$' | sort)
    local count=$(echo "$files" | wc -l)

    [ "$count" -gt "$retain" ] && echo "$files" | head -n $((count - retain)) | \
    while read -r old_log; do
        [ -n "$old_log" ] && {
            log_info "Remove old log file $log_folder/$old_log"
            rm -f "$log_folder/$old_log"
        }
    done

    return 0
}

cleanup() {
    local exit_code=$?

    if [ "$exit_code" -ne 0 ]; then
        log_info "Program terminated unexpectedly, code: $exit_code"
    fi

    if [ -n "$TARGET_FOLDER" ] && [ -d "$TARGET_FOLDER" ] && [ -f "$TARGET_FOLDER/$JOB_PROCESSING" ]; then
        log_info "Cleaning uncompleted backup..."
        rm -rf "$TARGET_FOLDER" 2>/dev/null
    fi

    release_pid_lock

    [ "$ENABLE_AUTOMOUNT" = "true" ] && umount_device

    log_info "Program exit with code $exit_code"
    exit $exit_code
}

main() {
    if [ "$(id -u)" -ne 0 ]; then
        log_error "This script must be run as root (UID 0)"
        exit 1
    fi

    parse_arguments "$@"

    check_config

    init_timestamp

    init_log

    set -e
    trap 'cleanup' EXIT INT TERM HUP

    mount_device

    prepare_job

    check_free_space

    acquire_pid_lock

    set_res_limits

    perform_job

    show_statistics

    clean_failure_backups
    
    clean_old_backups

    clean_old_logs

    exit 0
}

main "$@"

# TODO list
# --verify: verify backup integrity
# --dry-run
# send mail on fail
# exclude dir inline comments and blank line
# security check for conf file

[michael@tw1612n-vm-centos.gj.com ~]$ 
