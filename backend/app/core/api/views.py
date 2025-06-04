from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status, permissions, viewsets
from .serializers import RegisterSerializer, DeliverySerializer
from app.core.models import Delivery
import logging
import traceback

logger = logging.getLogger(__name__)

class RegisterView(APIView):
    permission_classes = (permissions.AllowAny,)

    def post(self, request):
        logger.debug(f"Received registration data: {request.data}")

        serializer = RegisterSerializer(data=request.data)
        if serializer.is_valid():
            try:
                user = serializer.save()
                logger.info(f"User registered successfully: {user.username}")
                return Response(
                    {"message": "User registered successfully"},
                    status=status.HTTP_201_CREATED
                )
            except Exception as e:
                trace = traceback.format_exc()
                traceback.print_exc()
                logger.error(f"Unexpected error during user registration: {e}", exc_info=True)
                return Response(
                    {
                        "error": str(e),
                        "trace": trace
                    },
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        else:
            logger.warning(f"Registration validation failed: {serializer.errors}")
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


# New Delivery ViewSet for CRUD
class DeliveryViewSet(viewsets.ModelViewSet):
    queryset = Delivery.objects.all()
    serializer_class = DeliverySerializer
    permission_classes = [permissions.IsAuthenticated]
