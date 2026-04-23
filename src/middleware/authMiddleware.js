import jwt from "jsonwebtoken";

const JWT_SECRET = "ooredoo_secret_key_123";

const authMiddleware = (req, res, next) => {
  const token = req.header("Authorization")?.replace("Bearer ", "");

  if (!token) {
    return res.status(401).json({ message: "No token, authorization denied" });
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    res.status(401).json({ message: "Token is not valid" });
  }
};

/**
 * Middleware to restrict access to admin users only.
 * Must be used AFTER authMiddleware (req.user must exist).
 */
const requireAdmin = (req, res, next) => {
  if (!req.user || req.user.role !== "admin") {
    return res.status(403).json({ message: "Access denied. Admin only." });
  }
  next();
};

export default authMiddleware;
export { requireAdmin };
