import numpy as np
import matplotlib.pyplot as plt

# 输入样本数据和子组大小
sample_data = [8, 9, 10, 11, 10, 9, 10, 11, 12, 8, 10, 12, 10, 9, 11, 10, 11, 9, 12, 10]
subgroup_size = 5

# 计算子组均值和P值
subgroup_means = [np.mean(sample_data[i:i+subgroup_size]) for i in range(0, len(sample_data), subgroup_size)]
p_values = [x / subgroup_size for x in subgroup_means]

# 设置控制限
mean_p = np.mean(p_values)
std_p = np.std(p_values)
UCL = mean_p + 3 * std_p
LCL = mean_p - 3 * std_p

# 绘制P控制图
plt.figure(figsize=(10, 6))
plt.plot(range(len(p_values)), p_values, marker='o', linestyle='-')
plt.axhline(UCL, color='red', linestyle='--', label='UCL')
plt.axhline(mean_p, color='green', linestyle='--', label='Mean')
plt.axhline(LCL, color='blue', linestyle='--', label='LCL')
plt.xlabel('Subgroup')
plt.ylabel('P Value')
plt.title('P Control Chart')
plt.legend()
plt.grid(True)

# 判异结果
out_of_control = any(p > UCL or p < LCL for p in p_values)
if out_of_control:
    print("存在异常点")
else:
    print("没有异常点")

plt.show()
