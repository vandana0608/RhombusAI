from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import ProcessedFileViewSet

router = DefaultRouter()
router.register(r'processed-files', ProcessedFileViewSet)

urlpatterns = [
    path('', include(router.urls)),
]