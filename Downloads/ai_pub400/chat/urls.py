from django.urls import path
from . import views


urlpatterns = [
    # ✅ Existing AI → DB2 UI
    path("", views.home, name="home"),
    path("ask/", views.ask, name="ask"),


    # (Optional) If you want uppercase URLs also to work:
    # path("SOURCE/", source_home),
    # path("SOURCE/api/", source_api),
]
