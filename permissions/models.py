from django.db import models
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin,Group, Permission
from django.utils.timezone import now
from facilities.models import Farm, Investor
from datetime import datetime
# Create your models here.



class AccountManager(BaseUserManager):
    def create_user(self, email, username, password=None, **extra_fields):
        """Tạo user thông thường."""
        if not email:
            raise ValueError("User must have an email address")
        if not username:
            raise ValueError("User must have a username")

        email = self.normalize_email(email)
        extra_fields.setdefault("is_active", True)  # Mặc định user được kích hoạt
        user = self.model(email=email, username=username, **extra_fields)
        user.set_password(password)
        user.save(using=self._db)
        return user
    
    def create_superuser(self, email, username=None, password=None, **extra_fields):
        """Tạo superuser với quyền admin."""
        if not username:
            raise ValueError("Superuser must have a username")  # Kiểm tra nếu username bị thiếu

        extra_fields.setdefault("is_superuser", True)
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("role", "admin")  # Đảm bảo role là admin

        return self.create_user(email=email, username=username, password=password, **extra_fields)
    def create_investor(self, email, username, password=None, **extra_fields):
        """Tạo investor dưới quyền superuser."""
        if not username:
            raise ValueError("Investor must have a username")

        extra_fields.setdefault("is_superuser", False)
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("role", "investor")

        # Tạo user trước
        user = self.create_user(email=email, username=username, password=password, **extra_fields)
        
        # Tìm hoặc tạo đối tượng Investor tương ứng
        try:
            investor_obj = Investor.objects.get(email=email)
            # Kiểm tra nếu investor đã tồn tại nhưng chưa có License thì tạo
            try:
                investor_obj.license_account
            except:
                investor_obj.generate_license()
        except Investor.DoesNotExist:
            investor_obj = Investor.objects.create(
                name=username,
                email=email,
                is_active=user.is_active
            )
            investor_obj.generate_license()
        
        # Liên kết Account với Investor
        user.investor_profile = investor_obj
        user.save()
        
        return user


    
class Account(AbstractBaseUser, PermissionsMixin):
    ROLE_CHOICES = (
        ('admin', 'Main Admin'),
        ('investor', 'Investor'),
        ('farm_admin', 'Farm Admin'),
        ('staff', 'Operator')
    )
    email= models.EmailField(verbose_name="Email", max_length= 255, unique= True)
    username = models.CharField(max_length= 30, unique= True)
    date_created = models.DateTimeField(verbose_name='date_created', default= now)
    last_login = models.DateTimeField(verbose_name='last_login', auto_now = True)
    is_active = models.BooleanField(default= True)
    is_staff = models.BooleanField(default= False)
    is_superuser = models.BooleanField(default= False)
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default='staff')
    
    # Thêm mối quan hệ đến Investor model
    investor_profile = models.ForeignKey(Investor, on_delete=models.SET_NULL, null=True, blank=True, related_name='accounts')
    
    # Giữ lại mối quan hệ phân cấp người dùng (một người có thể quản lý nhiều người khác)
    manager = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='managed_accounts')
    farm = models.ForeignKey(Farm, on_delete= models.CASCADE, null= True, blank= True)
    groups = models.ManyToManyField(Group, related_name="account_groups", blank=True)
    user_permissions = models.ManyToManyField(Permission, related_name="account_permissions", blank=True)
    USERNAME_FIELD = 'username'
    REQUIRED_FIELDS = ['email']
    
    objects = AccountManager()
    
    def __str__(self):
        return f"{self.username} ({self.role})"
    def has_perm(self, perm, obj=None):
        return self.is_superuser or self.is_staff
    def has_module_perms(self, app_label):
        return True
    
# License model
class License(models.Model):
    investor = models.OneToOneField(Investor, on_delete=models.CASCADE, related_name='license_account')
    key = models.CharField(max_length=64, unique=True)
    is_permanent = models.BooleanField(default=True)
    expiry_date = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def is_valid(self):
        """Kiểm tra license còn hạn không"""
        if self.is_permanent:
            return True
        return self.expiry_date and self.expiry_date > datetime.now()
# Create your models here.
