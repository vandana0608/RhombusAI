from django.db import models

class ProcessedFile(models.Model):
    original_file = models.FileField(upload_to='uploads/')
    processed_file = models.FileField(upload_to='processed/', null=True, blank=True)
    upload_date = models.DateTimeField(auto_now_add=True)
    processing_complete = models.BooleanField(default=False)

    def __str__(self):
        return f"ProcessedFile {self.id}"