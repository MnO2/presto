import os

success_count: int = 0
total: int = 10

for i in range(total):
    return_code = os.system('presto-cli/target/presto-cli-*-executable.jar --catalog tpch --schema sf1 --execute "select * from customer limit 10;"')
    if return_code == 0:
        success_count += 1

print(f"success: {success_count}, total: {total}")
