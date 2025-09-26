# DABOps - Databricks Asset Bundle Operations 🚀

**Databricks Asset Bundle Operations (DABOps)** is a comprehensive Streamlit application that simplifies the process of generating Databricks Asset Bundles from existing workflows. Transform your Databricks jobs into portable, version-controlled asset bundles with just a few clicks.

## 🌟 Features

- **📂 Workflow Discovery**: Automatically discover and list all workflows in your Databricks workspace
- **🎯 Interactive Selection**: User-friendly interface for selecting workflows to bundle
- **📦 Bundle Generation**: Generate standardized asset bundles with proper YAML configuration
- **💾 Workspace Integration**: Save generated bundles directly to your Databricks workspace
- **🔒 Secure Authentication**: Seamless integration with Databricks authentication
- **🎨 Modern UI**: Clean, intuitive Streamlit interface with real-time feedback
- **📊 Comprehensive Logging**: Detailed logging for monitoring and troubleshooting
- **⚙️ Configurable**: Extensive configuration options for customization

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Databricks workspace access
- Databricks CLI or SDK configured
- Required permissions: `workspace:read`, `jobs:read`, `workspace:write`

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd dabops-bot-agent
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Databricks authentication**:
   ```bash
   databricks configure --token
   ```
   Or set environment variables:
   ```bash
   export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-personal-access-token"
   ```

4. **Run the application**:
   ```bash
   streamlit run app.py
   ```

5. **Access the app**:
   Open your browser to `http://localhost:8501`

## 📱 Usage Guide

### 1. Authentication
- Ensure you're authenticated to your Databricks workspace
- The app will display your authentication status in the sidebar

### 2. Workflow Discovery
- The app automatically lists all workflows in your workspace
- Use the search function to filter workflows by name or description
- Sort workflows by name, creation date, or last modified date

### 3. Workflow Selection
- Browse workflows in the interactive table
- Select a single workflow using the checkbox
- View detailed workflow information in the expanded details section

### 4. Bundle Generation
- Configure bundle settings (name, dependencies, etc.)
- Click "Generate Asset Bundle" to create the bundle
- Preview the generated YAML configuration
- Download or save the bundle to your workspace

### 5. Bundle Management
- Generated bundles are saved to `/Workspace/Users/{user}/DABOps/`
- Download bundles for local development
- Integrate with CI/CD pipelines using the generated YAML

## 🏗️ Architecture

The application is built with a modular architecture:

```
DABOps/
├── app.py                 # Main Streamlit application
├── databricks_client.py   # Databricks API client
├── bundle_generator.py    # Asset bundle generation logic
├── config.py             # Configuration management
├── utils.py              # Utility functions
├── requirements.txt      # Python dependencies
└── .databricks-app.yml   # Databricks App configuration
```

### Key Components

- **`app.py`**: Main application entry point with Streamlit UI
- **`databricks_client.py`**: Handles all Databricks API interactions
- **`bundle_generator.py`**: Converts workflows to asset bundle format
- **`config.py`**: Centralized configuration management
- **`utils.py`**: Common utilities and helper functions

## ⚙️ Configuration

### Environment Variables

```bash
# Databricks Configuration
DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
DATABRICKS_TOKEN="your-token"
DATABRICKS_CONFIG_PROFILE="DEFAULT"

# App Configuration
DABOPS_LOG_LEVEL="INFO"
DABOPS_MAX_WORKFLOWS="100"
DABOPS_BUNDLE_FORMAT="yaml"
DABOPS_AUTO_SAVE="true"
DABOPS_THEME="light"
DABOPS_CACHE_TTL="300"
DABOPS_DEBUG="false"
DABOPS_TELEMETRY="true"
```

### Configuration File

Create a `config.json` file for persistent settings:

```json
{
  "max_workflows_display": 100,
  "default_bundle_format": "yaml",
  "auto_save_bundles": true,
  "theme": "light",
  "log_level": "INFO",
  "enable_debug_mode": false,
  "enable_telemetry": true
}
```

## 🔧 Development

### Project Structure

```
DABOps/
├── app.py                    # Main application
├── databricks_client.py      # API client
├── bundle_generator.py       # Bundle logic
├── config.py                # Configuration
├── utils.py                 # Utilities
├── requirements.txt         # Dependencies
├── .databricks-app.yml      # App config
├── .gitignore              # Git ignore
└── tests/                  # Test files
    ├── test_client.py
    ├── test_generator.py
    └── test_utils.py
```

### Running Tests

```bash
pytest tests/ -v
```

### Code Quality

```bash
# Format code
black *.py

# Lint code
flake8 *.py

# Type checking (optional)
mypy *.py
```

## 🚀 Deployment

### Databricks Apps Deployment

1. **Prepare the application**:
   ```bash
   # Ensure all files are under 10MB total
   du -sh .
   ```

2. **Create app package**:
   ```bash
   # Remove development files
   rm -rf tests/ .git/ *.md
   ```

3. **Deploy to Databricks**:
   ```bash
   databricks apps create \
     --name "DABOps" \
     --description "Asset Bundle Operations" \
     --source-code-path .
   ```

### Docker Deployment (Alternative)

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

## 📊 Monitoring & Logging

### Log Levels

- **DEBUG**: Detailed debug information
- **INFO**: General application information
- **WARNING**: Warning messages
- **ERROR**: Error messages
- **CRITICAL**: Critical system errors

### Metrics

The application tracks:
- Number of workflows processed
- Bundle generation success/failure rates
- User authentication events
- Performance metrics

## 🔒 Security

### Authentication
- Supports Databricks OAuth and Personal Access Tokens
- Automatic token validation and refresh
- Secure storage of credentials

### Permissions
Required Databricks permissions:
- `workspace:read` - List and read workspace objects
- `jobs:read` - Read job configurations
- `workspace:write` - Save generated bundles

### Data Privacy
- No user data is stored locally
- All operations are performed within the Databricks workspace
- Logs can be configured to exclude sensitive information

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add docstrings to all functions
- Include unit tests for new features
- Update documentation as needed

## 📚 Documentation

- [User Guide](docs/user-guide.md) - Detailed usage instructions
- [Developer Guide](docs/developer-guide.md) - Development setup and guidelines
- [API Documentation](docs/api.md) - API reference
- [Troubleshooting](docs/troubleshooting.md) - Common issues and solutions

## 🐛 Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Verify Databricks token is valid
   - Check workspace URL format
   - Ensure proper permissions

2. **Bundle Generation Failed**
   - Check workflow configuration
   - Verify job permissions
   - Review application logs

3. **Performance Issues**
   - Reduce max workflows limit
   - Enable caching
   - Check network connectivity

For more detailed troubleshooting, see [Troubleshooting Guide](docs/troubleshooting.md).

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Databricks team for the excellent SDK and CLI tools
- Streamlit community for the amazing framework
- Contributors and users who make this project better

## 📞 Support

- 🐛 **Bug Reports**: [GitHub Issues](https://github.com/your-org/dabops-bot-agent/issues)
- 💡 **Feature Requests**: [GitHub Discussions](https://github.com/your-org/dabops-bot-agent/discussions)
- 📧 **Email**: support@databricks.com
- 📚 **Documentation**: [Databricks Docs](https://docs.databricks.com/dev-tools/bundles/)

---

**Made with ❤️ by the Databricks Community**
