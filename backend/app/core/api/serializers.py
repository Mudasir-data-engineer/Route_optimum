from django.contrib.auth import get_user_model
from rest_framework import serializers
from app.core.models import Delivery

User = get_user_model()

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'username', 'email', 'role', 'contact_info']

class RegisterSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True)
    role = serializers.ChoiceField(choices=User.ROLE_CHOICES)

    class Meta:
        model = User
        fields = ['username', 'email', 'password', 'role', 'contact_info']

    def create(self, validated_data):
        try:
            return User.objects.create_user(
                username=validated_data['username'],
                email=validated_data.get('email', ''),
                password=validated_data['password'],
                role=validated_data.get('role', 'customer'),
                contact_info=validated_data.get('contact_info', '')
            )
        except Exception as e:
            raise serializers.ValidationError(f"Failed to create user: {e}")

class DeliverySerializer(serializers.ModelSerializer):
    class Meta:
        model = Delivery
        fields = ['id', 'delivery_id', 'customer_name', 'address', 'delivery_date', 'status']
