import React, { useState } from 'react';
import { apiRequest } from '../api/api';

export default function Register() {
  const [formData, setFormData] = useState({
    username: '',
    email: '',
    password: '',
    role: 'customer',
    contact_info: '',
  });
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');

  const handleChange = (e) => {
    setFormData(prev => ({ ...prev, [e.target.name]: e.target.value }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setSuccess('');
    try {
      await apiRequest('/core/register/', 'POST', formData);
      setSuccess('Registration successful! You can now login.');
      setFormData({
        username: '',
        email: '',
        password: '',
        role: 'customer',
        contact_info: '',
      });
    } catch (err) {
      setError(err.message);
    }
  };

  return (
    <form onSubmit={handleSubmit}>
      <h2>Register</h2>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      {success && <p style={{ color: 'green' }}>{success}</p>}
      <input
        name="username"
        placeholder="Username"
        value={formData.username}
        onChange={handleChange}
        required
      /><br />
      <input
        type="email"
        name="email"
        placeholder="Email"
        value={formData.email}
        onChange={handleChange}
        required
      /><br />
      <input
        type="password"
        name="password"
        placeholder="Password"
        value={formData.password}
        onChange={handleChange}
        required
      /><br />
      <input
        name="contact_info"
        placeholder="Contact Info"
        value={formData.contact_info}
        onChange={handleChange}
      /><br />
      <select name="role" value={formData.role} onChange={handleChange}>
        <option value="customer">Customer</option>
        <option value="driver">Driver</option>
        <option value="dispatcher">Dispatcher</option>
        <option value="admin">Admin</option>
      </select><br />
      <button type="submit">Register</button>
    </form>
  );
}
