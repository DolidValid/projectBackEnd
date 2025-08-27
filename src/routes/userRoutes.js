import { Router } from "express";
import addUser  from "../controllers/userController.js";

/**
 * Express router instance for handling user-related routes.
 * @type {import('express').Router}
 */
const router = Router();

router.post("/add-user", addUser);

export default router;
