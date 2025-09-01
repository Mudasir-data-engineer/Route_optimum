# ---------- app/deliveries/api/serializers.py ----------
from rest_framework import serializers
from app.core.models import User
from app.deliveries.models import Delivery, Route   # ✅ correct import path


class RegisterSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ['id', 'username', 'password', 'email', 'full_name', 'role', 'contact_info']
        extra_kwargs = {'password': {'write_only': True}}

    def create(self, validated_data):
        user = User(
            username=validated_data['username'],
            email=validated_data['email'],
            full_name=validated_data['full_name'],
            role=validated_data['role'],
            contact_info=validated_data.get('contact_info', '')
        )
        user.set_password(validated_data['password'])
        user.save()
        return user


class DeliverySerializer(serializers.ModelSerializer):
    class Meta:
        model = Delivery
        fields = '__all__'


class RouteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Route
        fields = '__all__'
