from pathlib import Path

print("Attaching all workloads present in folder /workload")
for file in Path().absolute().glob("**/*.py"):
    print(file)
