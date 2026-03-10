# 使用 SSH 发布到 GitHub

## SSH 配置已确认

- SSH 密钥: `~/.ssh/id_ed25519_lzusa_github`
- GitHub 用户名: `lzusa`
- 认证状态: 已通过 `ssh -T git@github.com` 验证

## 快速发布步骤

### Step 1: 在 GitHub 创建仓库

访问: https://github.com/new

填写:
- Repository name: `th123-replay-recorder`
- Description: `Automatic replay recorder for Touhou 12.3 (Hisoutensoku)`
- 选择 **Public**
- **不要勾选** "Add a README file"
- 点击 **Create repository**

### Step 2: 推送代码

在终端执行:

```bash
# 加载 SSH 密钥
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519_lzusa_github

# 进入项目目录
cd /home/HwHiAiUser/workspace/th123-replay-recorder

# 设置远程仓库
git remote remove origin 2>/dev/null || true
git remote add origin git@github.com:lzusa/th123-replay-recorder.git

# 推送代码
git push -u origin master
```

### Step 3: 验证

访问: https://github.com/lzusa/th123-replay-recorder

## 项目文件说明

```
th123-replay-recorder/
├── replay_recorder.py    # 主脚本 (1844 行)
├── README.md             # 详细文档 (包含协议和 rep 格式)
├── PUBLISH_COMPLETE.md   # 完整发布指南
└── PUBLISH_SSH.md        # 本文件
```

## 可选: 添加 LICENSE

```bash
cd /home/HwHiAiUser/workspace/th123-replay-recorder
curl -s https://raw.githubusercontent.com/github/choosealicense.com/gh-pages/_licenses/mit.txt > LICENSE
git add LICENSE
git commit -m "Add MIT license"
git push
```

## 可选: 创建 Release

```bash
# 创建标签
git tag -a v1.0.0 -m "Initial release"
git push origin v1.0.0
```

然后在 GitHub 页面创建 Release。
