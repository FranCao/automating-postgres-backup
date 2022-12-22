import os
import subprocess
import boto3
from datetime import datetime

# Source database details
source_hostname = os.environ['source_hostname']
source_username = os.environ['source_username']
source_password = os.environ['source_password']
source_database = os.environ['source_database']
# Target database details
target_hostname = os.environ['target_hostname']
target_username = os.environ['target_username']
target_password = os.environ['target_password']
target_database = os.environ['target_database']

dump_success = None

# Optional: Connect to S3 to store database backups
s3 = boto3.resource('s3')
# Replace with your S3 bucket name
bucket = s3.Bucket('database-backup')

# Run pg_dump with subprocess to download everything from the source database
print('Starting database dump')
dump_command = f'pg_dump --host={source_hostname} --dbname={source_database} --username={source_username} --no-password --file=/tmp/database_backup.dmp --format=custom'

try:
    dump_proc = subprocess.Popen(dump_command, shell=True, env={'PGPASSWORD':source_password})
    dump_proc.wait()
    dump_success = 1
    print('Database dump successful')

except Exception as e:
    dump_success = 0
    print(f'Exception happened during pg_dump {e}')

# Save the backup to S3
if dump_success:
    try:
        data = open('/tmp/database_backup.dmp', 'rb')
        key = 'database_backup_' + datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        bucket.put_object(Key=key, Body=data)
        print(f'Database dump successfully backed up to S3 {bucket}')
    except Exception as e:
        print(f'Exception happened during upload to S3 {e}')

# Run pg_restore with subprocess to restore backup to target database
print('Starting database restore')
backup_file = '/tmp/database_backup.dmp'
dump_success = 1
if dump_success:
    try:
        restore_proc = subprocess.Popen(['pg_restore','--no-owner', '--clean', '--dbname=postgresql://{}:{}@{}:{}/{}'.format(target_username,target_password,target_hostname,'5432',target_database), '-v', backup_file], stdout=subprocess.PIPE)
        output = restore_proc.communicate()[0]
        print('Database restore to target successful')
    except Exception as e:
        print(f'Exception during restore to RDS {e}')
