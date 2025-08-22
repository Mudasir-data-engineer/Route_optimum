# app/core/models.py
from django.contrib.auth.models import AbstractUser, Group, Permission
from django.db import models

class User(AbstractUser):
    ROLE_CHOICES = (
        ('admin', 'Admin'),
        ('driver', 'Driver'),
        ('dispatcher', 'Dispatcher'),
        ('customer', 'Customer'),
    )

    full_name = models.CharField(max_length=150)  # ✅ matches DB
    role = models.CharField(max_length=20, choices=ROLE_CHOICES)
    contact_info = models.TextField(blank=True, null=True)  # optional extra field

    groups = models.ManyToManyField(
        Group,
        related_name='core_user_set',  # avoid clash with auth.User
        blank=True,
        help_text='The groups this user belongs to.',
        verbose_name='groups'
    )
    user_permissions = models.ManyToManyField(
        Permission,
        related_name='core_user_set',  # avoid clash with auth.User
        blank=True,
        help_text='Specific permissions for this user.',
        verbose_name='user permissions'
    )

    def __str__(self):
        return self.username
