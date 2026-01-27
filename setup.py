"""DataGen 安装配置"""
from setuptools import setup, find_packages
import os

# 读取 README
readme_path = os.path.join(os.path.dirname(__file__), "README.md")
long_description = ""
if os.path.exists(readme_path):
    with open(readme_path, "r", encoding="utf-8") as f:
        long_description = f.read()

setup(
    name="llm-datagen",
    version="1.0.0",
    description="极简高性能流式数据加工库",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="llm-datagen Team",
    author_email="",
    url="https://github.com/your-org/llm-datagen",
    packages=find_packages(where=".", include=["llm_datagen*"]),
    package_dir={"": "."},
    python_requires=">=3.8",
    install_requires=[
        "openai>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="data processing, pipeline, llm, streaming",
)
