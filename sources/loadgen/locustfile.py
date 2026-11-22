"""Steady shop traffic: half browsing (gateway -> inventory), half checkouts (gateway -> orders -> ...)."""
from locust import HttpUser, between, task


class Shopper(HttpUser):
    wait_time = between(0.5, 2)

    @task
    def browse(self):
        self.client.get("/products")

    @task
    def checkout(self):
        self.client.get("/checkout")
