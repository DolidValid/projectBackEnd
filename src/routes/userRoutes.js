import { Router } from "express";
import { addUser } from "../controllers/userController";

const router = Router();

router.post("/add-user", addUser);

export default router;
