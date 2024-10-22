import os
from django.conf import settings
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from .models import ProcessedFile
from .serializers import ProcessedFileSerializer
from .data_processor import process_file  # Import your data processing function

class ProcessedFileViewSet(viewsets.ModelViewSet):
    queryset = ProcessedFile.objects.all()
    serializer_class = ProcessedFileSerializer

    @action(detail=True, methods=['post'])
    def process(self, request, pk=None):
        processed_file = self.get_object()
        if processed_file.processing_complete:
            return Response({"message": "File has already been processed."}, status=status.HTTP_400_BAD_REQUEST)

        input_file_path = os.path.join(settings.MEDIA_ROOT, str(processed_file.original_file))
        output_file_name = f"processed_{os.path.basename(str(processed_file.original_file))}"
        output_file_path = os.path.join(settings.MEDIA_ROOT, 'processed', output_file_name)

        try:
            # Process the file using your data processing function
            df = process_file(input_file_path)
            
            # Save the processed DataFrame to a new CSV file
            df.to_csv(output_file_path, index=False)
            
            # Update the ProcessedFile object
            processed_file.processed_file.name = f"processed/{output_file_name}"
            processed_file.processing_complete = True
            processed_file.save()

            serializer = self.get_serializer(processed_file)
            return Response(serializer.data)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)