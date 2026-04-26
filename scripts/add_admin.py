"""添加管理员账号（手机号+密码）。用法: python scripts/add_admin.py [phone] [password]"""
import sys

# 确保项目根目录在 sys.path
sys.path.insert(0, ".")

from app.core.db import SessionLocal
from app.core.security import hash_password
from app.models import User


def add_admin(phone: str, password: str) -> bool:
    """添加或更新管理员。若手机号已存在则更新密码和角色。"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.phone == phone).first()
        if user:
            user.password_hash = hash_password(password)
            user.role = "admin"
            db.commit()
            print(f"已更新管理员: {phone} (role=admin)")
            return True
        email = f"{phone}@phone.local"
        if db.query(User).filter(User.email == email).first():
            # 可能是用 email 注册的，尝试更新
            user = db.query(User).filter(User.email == email).first()
            user.phone = phone
            user.password_hash = hash_password(password)
            user.role = "admin"
            db.commit()
            print(f"已更新管理员: {phone} (绑定手机号并设为 admin)")
            return True
        user = User(
            email=email,
            phone=phone,
            password_hash=hash_password(password),
            role="admin",
            membership_level="free",
            membership_expires_at=None,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        print(f"已创建管理员: {phone} (id={user.id})")
        return True
    except Exception as e:
        db.rollback()
        print(f"错误: {e}")
        return False
    finally:
        db.close()


if __name__ == "__main__":
    phone = sys.argv[1] if len(sys.argv) > 1 else "18151199096"
    password = sys.argv[2] if len(sys.argv) > 2 else "Qwer1234.."
    if len(phone) != 11 or not phone.isdigit():
        print("手机号需为11位数字")
        sys.exit(1)
    if len(password) < 8:
        print("密码需至少8位")
        sys.exit(1)
    ok = add_admin(phone, password)
    sys.exit(0 if ok else 1)
