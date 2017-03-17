# List of valid osd flags
OSD_FLAGS = [
    'pause', 'noup', 'nodown', 'noout', 'noin', 'nobackfill',
    'norecover', 'noscrub', 'nodeep-scrub',
]

# Implemented osd commands
OSD_IMPLEMENTED_COMMANDS = ['scrub', 'deep_scrub', 'repair']

# Valid values for the 'var' argument to 'ceph osd pool set'
POOL_PROPERTIES = [
    'size', 'min_size', 'crash_replay_interval', 'pg_num',
    'pgp_num', 'crush_ruleset', 'hashpspool',
]

# Valid values for the 'ceph osd pool set-quota' command
POOL_QUOTA_PROPERTIES = [
    ('quota_max_bytes', 'max_bytes'),
    ('quota_max_objects', 'max_objects'),
]
