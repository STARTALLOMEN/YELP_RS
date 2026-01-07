import os

hadoop_home = os.environ.get('HADOOP_HOME')
print(f"HADOOP_HOME: {hadoop_home}")

if hadoop_home:
    winutils_path = os.path.join(hadoop_home, 'bin', 'winutils.exe')
    if os.path.exists(winutils_path):
        print(f"Found winutils.exe at: {winutils_path}")
    else:
        print(f"Missing winutils.exe at: {winutils_path}")
else:
    print("HADOOP_HOME environment variable is NOT set.")
