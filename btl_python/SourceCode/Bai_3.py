import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv('results.csv')

df_numeric = df.select_dtypes(include=[np.number])
imputer = SimpleImputer(strategy='mean')
df_imputed = pd.DataFrame(imputer.fit_transform(df_numeric), columns=df_numeric.columns)

scaler = StandardScaler()
df_scaled = scaler.fit_transform(df_imputed)

inertias = []
silhouette_scores = []
K = range(2, 11)

for k in K:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = kmeans.fit_predict(df_scaled)
    inertias.append(kmeans.inertia_)
    silhouette_scores.append(silhouette_score(df_scaled, labels))

fig, ax = plt.subplots(1, 2, figsize=(14, 5))

ax[0].plot(K, inertias, marker='o')
ax[0].set_title('Elbow Method')
ax[0].set_xlabel('Số cụm (k)')
ax[0].set_ylabel('Inertia')
ax[0].grid(True)

ax[1].plot(K, silhouette_scores, marker='o', color='green')
ax[1].set_title('Silhouette Score')
ax[1].set_xlabel('Số cụm (k)')
ax[1].set_ylabel('Silhouette Score')
ax[1].grid(True)

plt.tight_layout()
plt.savefig("elbow_silhouette.png")  
plt.show()

best_k = 3
kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
clusters = kmeans.fit_predict(df_scaled)

pca = PCA(n_components=2)
df_pca = pca.fit_transform(df_scaled)

df_plot = pd.DataFrame(df_pca, columns=["PC1", "PC2"])
df_plot["Cluster"] = clusters

plt.figure(figsize=(8, 6))
sns.scatterplot(data=df_plot, x="PC1", y="PC2", hue="Cluster", palette="Set2", s=60)
plt.title(f"Phân cụm cầu thủ với KMeans (k={best_k}) sau PCA")
plt.xlabel("Thành phần chính 1")
plt.ylabel("Thành phần chính 2")
plt.grid(True)
plt.tight_layout()
plt.savefig("player_clusters_pca.png") 
plt.show()
