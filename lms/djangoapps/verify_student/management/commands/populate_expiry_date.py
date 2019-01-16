"""
Django admin command to populate expiry_date for approved verifications in SoftwareSecurePhotoVerification
"""
import logging
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand

from lms.djangoapps.verify_student.models import SoftwareSecurePhotoVerification

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    This command sets the expiry_date for users for which the verification is approved

    The task is performed in batches with maximum number of rows to process given in argument `batch_size`

    Default values:
        `batch_size` = 1000 rows

    Example usage:
        $ ./manage.py lms populate_expiry_date --batch_size=1000
    OR
        $ ./manage.py lms populate_expiry_date
    """
    help = 'Populate expiry_date for approved verifications'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch_size',
            action='store',
            dest='batch_size',
            type=int,
            default=1000,
            help='Maximum number of database rows to process. '
                 'This helps avoid locking the database while updating large amount of data.')

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self.sspv = SoftwareSecurePhotoVerification.objects.filter(status='approved').order_by('-user_id')

    def handle(self, *args, **options):
        """
        Handler for the command

        It creates batches of approved Software Secure Photo Verification depending on the batch_size
        given as argument. Then for each distinct user in that batch it finds the most recent approved
        verification and sets it expiry_date
        """
        batch_size = options['batch_size']

        try:
            max_user_id = self.sspv[0].user_id
            batch_start = self.sspv.reverse()[0].user_id
            batch_stop = batch_start + batch_size
        except IndexError:
            logger.info("IndexError: No approved entries found in SoftwareSecurePhotoVerification")
            return

        while batch_start <= max_user_id:
            batch_queryset = self.sspv.filter(user_id__gte=batch_start, user_id__lt=batch_stop)
            users = batch_queryset.order_by().values('user_id').distinct()

            for user in users:
                recent_verification = self.find_recent_verification(user['user_id'])
                recent_verification.expiry_date = recent_verification.updated_at + timedelta(
                        days=settings.VERIFY_STUDENT["DAYS_GOOD_FOR"])
                recent_verification.save()
                logger.warning(recent_verification.updated_at)

            batch_start = batch_stop
            batch_stop += batch_size

    def find_recent_verification(self, user_id):
        """
        Returns the most recent approved verification for a user
        """
        return self.sspv.filter(user_id=user_id).latest('updated_at')
