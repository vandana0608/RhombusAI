from rest_framework import serializers
from .models import ProcessedFile

class ProcessedFileSerializer(serializers.ModelSerializer):
    class Meta:
        model = ProcessedFile
        fields = ['id', 'original_file', 'processed_file', 'upload_date', 'processing_complete']