from django.contrib.auth.models import AbstractUser, Group, Permission
from django.db import models

class User(AbstractUser):
    ROLE_CHOICES = (
        ('admin', 'Admin'),
        ('driver', 'Driver'),
        ('dispatcher', 'Dispatcher'),
        ('customer', 'Customer'),
    )

    role = models.CharField(max_length=20, choices=ROLE_CHOICES)
    contact_info = models.TextField(blank=True, null=True)

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


class Delivery(models.Model):
    # Example fields - customize as needed
    delivery_id = models.CharField(max_length=50, unique=True)
    customer_name = models.CharField(max_length=100)
    address = models.TextField()
    delivery_date = models.DateField()
    status = models.CharField(max_length=20, default='pending')

    def __str__(self):
        return f"Delivery {self.delivery_id} to {self.customer_name}"
