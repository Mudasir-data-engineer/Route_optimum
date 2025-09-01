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

    full_name = models.CharField(max_length=150)
    role = models.CharField(max_length=20, choices=ROLE_CHOICES)
    contact_info = models.TextField(blank=True, null=True)

    groups = models.ManyToManyField(
        Group,
        related_name='core_user_set',
        blank=True,
        help_text='The groups this user belongs to.',
        verbose_name='groups'
    )
    user_permissions = models.ManyToManyField(
        Permission,
        related_name='core_user_set',
        blank=True,
        help_text='Specific permissions for this user.',
        verbose_name='user permissions'
    )

    def __str__(self):
        return self.username


class Delivery(models.Model):
    STATUS_CHOICES = (
        ('pending', 'Pending'),
        ('in_transit', 'In Transit'),
        ('delivered', 'Delivered'),
        ('canceled', 'Canceled'),
    )

    sender = models.ForeignKey(User, related_name='sent_deliveries', on_delete=models.CASCADE)
    receiver = models.ForeignKey(User, related_name='received_deliveries', on_delete=models.CASCADE)
    pickup_address = models.CharField(max_length=255)
    dropoff_address = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Delivery #{self.id} from {self.sender} to {self.receiver} - {self.status}"
