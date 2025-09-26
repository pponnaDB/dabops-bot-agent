# DABOps Demo Instructions 🚀

This document provides instructions for demonstrating the DABOps application.

## Prerequisites

1. **Virtual Environment**: Ensure you're in the virtual environment
   ```bash
   source venv/bin/activate
   ```

2. **Dependencies**: All dependencies should be installed from requirements.txt

## Demo Scenarios

### Scenario 1: Local Demo (Without Databricks Connection)

For demonstration purposes when you don't have Databricks authentication set up:

```bash
# Set demo mode environment variable
export DABOPS_DEBUG=true

# Run the application
streamlit run app.py
```

The app will show authentication warnings but you can still see the UI components.

### Scenario 2: Full Demo (With Databricks Authentication)

For a complete demonstration with real Databricks workspace:

```bash
# Set your Databricks credentials
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"

# Optional: Set other configuration
export DABOPS_MAX_WORKFLOWS="50"
export DABOPS_LOG_LEVEL="DEBUG"

# Run the application
streamlit run app.py
```

## Features to Demonstrate

### 1. Application Layout
- **Header**: Shows app title and workspace connection status
- **Sidebar**: Navigation, authentication status, and settings
- **Main Area**: Workflow discovery, selection, and bundle generation

### 2. Workflow Discovery
- Lists all workflows from the workspace
- Search functionality to filter workflows
- Sortable table with workflow details
- Real-time data refresh

### 3. Workflow Selection
- Interactive table with checkboxes
- Single workflow selection validation
- Detailed workflow information display
- Expandable details section

### 4. Bundle Generation
- Bundle name configuration
- Dependency inclusion options
- Real-time YAML generation
- Preview of generated bundle

### 5. File Operations
- Download generated bundles
- Save to workspace functionality
- Path management and validation

## Demo Script

### Introduction (2-3 minutes)
1. "Welcome to DABOps - Databricks Asset Bundle Operations"
2. Explain the problem: Converting existing workflows to asset bundles
3. Show the main application interface

### Workflow Discovery (3-4 minutes)
1. Navigate to the workflow discovery section
2. Demonstrate the search functionality
3. Show sorting options (by name, date, etc.)
4. Explain the workflow information displayed

### Workflow Selection (2-3 minutes)
1. Select a workflow from the list
2. Show the detailed workflow information
3. Explain task structure and dependencies
4. Highlight the validation features

### Bundle Generation (5-7 minutes)
1. Configure bundle settings
2. Generate the asset bundle
3. Show the YAML preview
4. Explain the structure of the generated bundle
5. Demonstrate download functionality
6. Show workspace save feature

### Advanced Features (3-5 minutes)
1. Configuration options in sidebar
2. Error handling demonstrations
3. Logging and monitoring features
4. Deployment considerations

## Key Talking Points

### Technical Architecture
- **Modular Design**: Separate concerns with dedicated modules
- **Databricks SDK Integration**: Native API integration
- **Error Handling**: Comprehensive error management
- **Streamlit UI**: Modern, responsive interface

### Business Value
- **Time Savings**: Automate bundle creation process
- **Consistency**: Standardized bundle structure
- **Migration**: Easy transition to asset bundles
- **DevOps Integration**: Ready for CI/CD pipelines

### Security & Compliance
- **Authentication**: Secure Databricks integration
- **Permissions**: Workspace-level access control
- **Data Privacy**: No external data storage
- **Audit Trail**: Comprehensive logging

## Troubleshooting Demo Issues

### Common Issues and Solutions

1. **Authentication Failed**
   ```bash
   # Check credentials
   echo $DATABRICKS_HOST
   echo $DATABRICKS_TOKEN
   
   # Test connection
   databricks workspace list
   ```

2. **No Workflows Displayed**
   ```bash
   # Check permissions
   databricks jobs list --limit 5
   ```

3. **Bundle Generation Errors**
   - Show error handling in the UI
   - Demonstrate validation messages
   - Explain recovery options

## Sample Data for Demo

If you need sample data for demonstration:

```bash
# Use the built-in sample data
export DABOPS_USE_SAMPLE_DATA=true
streamlit run app.py
```

This will populate the interface with realistic sample workflows for demonstration purposes.

## Performance Notes

- **Caching**: The app uses Streamlit caching for better performance
- **Pagination**: Large workflow lists are handled efficiently
- **Resource Usage**: Minimal resource requirements
- **Scalability**: Handles workspaces with hundreds of workflows

## Post-Demo Resources

After the demo, provide attendees with:

1. **GitHub Repository**: Link to the complete source code
2. **Documentation**: Comprehensive setup and usage guide
3. **Installation Scripts**: Automated setup procedures
4. **Support Channels**: How to get help and report issues

## Next Steps

Potential follow-up demonstrations or discussions:

1. **Custom Bundle Templates**: Extending the generator
2. **CI/CD Integration**: Automated deployment workflows
3. **Multi-Environment**: Supporting dev/staging/prod
4. **Advanced Features**: Custom validation, template management

---

**Happy Demoing! 🎉**
