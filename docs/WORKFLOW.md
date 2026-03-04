# Development Workflow

This project uses a Double Helix Development Model.

Two AI agents work in parallel:

- GitHub Copilot
- Cursor AI

Each development task is implemented twice.

--------------------------------------------------

Workflow

1. Create two branches from main

taskXXX-copilot
taskXXX-cursor

2. Implement the same task in both branches

3. Compare the implementations

4. Merge the better implementation into main

5. Repeat for the next task

--------------------------------------------------

Rules

• Never modify main directly  
• Always create a task branch  
• Keep Docker / pytest / flake8 working  
• Do not remove existing functionality unless explicitly required

--------------------------------------------------

Project Goal

The primary goal is to build a working archive application.

AI comparison is a secondary goal.

Stability and correctness are more important than novelty.

--------------------------------------------------

Japanese Summary

このプロジェクトは **二重螺旋開発モデル** を採用しています。

Copilot と Cursor の2つのAIが同じタスクを実装します。

両方の実装を比較し、より良いものを main にマージします。

