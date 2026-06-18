from sklearn.datasets import fetch_20newsgroups
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer

# 1 加载数据
news = fetch_20newsgroups(subset='all')
print(news.keys())
# 2 数据预处理
x_train, x_test, y_train, y_test = train_test_split(news.data, news.target, test_size=0.2)

tfidVectorizer = TfidfVectorizer()# 特征提取TF-IDF
x_train_scaled = tfidVectorizer.fit_transform(x_train) #fit计算生成模型
x_tesst_scaled = tfidVectorizer.transfrom(x_test) # 使用训练集的参数转换测试集

# 3 创建和训练Multinorminal模型
