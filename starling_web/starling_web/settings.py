from __future__ import annotations

import os
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured
from dotenv import load_dotenv


APP_DIR = Path(__file__).resolve().parent
BASE_DIR = APP_DIR.parent
PROJECT_ROOT = BASE_DIR.parent

load_dotenv(PROJECT_ROOT / ".env")


def _get_env(name):
    value = os.getenv(name)
    if value is None:
        raise ImproperlyConfigured(f"{name} is required")
    cleaned = value.strip()
    if not cleaned:
        raise ImproperlyConfigured(f"{name} cannot be blank")
    return cleaned


def _parse_bool(value):
    lowered = value.lower()
    if lowered in {"1", "true", "yes"}:
        return True
    if lowered in {"0", "false", "no"}:
        return False
    raise ImproperlyConfigured(f"Unable to interpret boolean value: {value}")


def _resolve_path(value):
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate


SECRET_KEY = _get_env("DJANGO_SECRET_KEY")
DEBUG = _parse_bool(_get_env("DJANGO_DEBUG"))
ALLOWED_HOSTS = [
    host.strip()
    for host in _get_env("DJANGO_ALLOWED_HOSTS").split(",")
    if host.strip()
]
if not ALLOWED_HOSTS and not DEBUG:
    raise ImproperlyConfigured("DJANGO_ALLOWED_HOSTS must contain at least one host when DEBUG is false")

STARLING_FEEDS_DB = _resolve_path(_get_env("STARLING_FEEDS_DB"))
STARLING_SUMMARY_DAYS = int(_get_env("STARLING_SUMMARY_DAYS"))
if STARLING_SUMMARY_DAYS <= 0:
    raise ImproperlyConfigured("STARLING_SUMMARY_DAYS must be positive")

# Application definition


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'starling_web.spaces',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'starling_web.starling_web.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [PROJECT_ROOT / 'templates'],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'starling_web.starling_web.wsgi.application'


# Database
# https://docs.djangoproject.com/en/5.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': _resolve_path(_get_env('DJANGO_DATABASE_PATH')),
    }
}


# Password validation
# https://docs.djangoproject.com/en/5.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.2/howto/static-files/

STATIC_URL = 'static/'

# Default primary key field type
# https://docs.djangoproject.com/en/5.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
