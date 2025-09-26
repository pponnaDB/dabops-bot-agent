"""
Databricks Asset Bundle Operations (DABOps) App
A Streamlit application for generating asset bundles from Databricks workflows.
"""

import streamlit as st
import pandas as pd
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

# Import custom modules
from databricks_client import DatabricksClient
from bundle_generator import BundleGenerator
from config import AppConfig
from utils import setup_logging, format_job_info, handle_error

# Configure logging
setup_logging()
logger = logging.getLogger(__name__)

class DABOpsApp:
    """Main application class for DABOps Streamlit app."""
    
    def __init__(self):
        """Initialize the application."""
        self.config = AppConfig()
        self.db_client = None
        self.bundle_generator = None
        
    def initialize_clients(self) -> bool:
        """Initialize Databricks client and bundle generator."""
        try:
            self.db_client = DatabricksClient()
            self.bundle_generator = BundleGenerator(self.db_client)
            return True
        except Exception as e:
            st.error(f"Failed to initialize Databricks connection: {str(e)}")
            logger.error(f"Client initialization error: {str(e)}")
            return False
    
    def render_header(self):
        """Render the application header."""
        st.set_page_config(
            page_title="DABOps - Databricks Asset Bundle Operations",
            page_icon="🚀",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        st.title("🚀 DABOps - Databricks Asset Bundle Operations")
        st.markdown("Generate asset bundles from your Databricks workflows with ease.")
        
        # Add workspace info if available
        if self.db_client and self.db_client.is_authenticated():
            workspace_info = self.db_client.get_workspace_info()
            if workspace_info:
                st.info(f"Connected to workspace: **{workspace_info.get('workspace_url', 'Unknown')}**")
    
    def render_sidebar(self):
        """Render the application sidebar."""
        with st.sidebar:
            st.header("📋 Navigation")
            
            # Authentication status
            if self.db_client and self.db_client.is_authenticated():
                st.success("✅ Authenticated")
            else:
                st.error("❌ Not Authenticated")
            
            # App information
            st.markdown("---")
            st.header("ℹ️ About")
            st.markdown("""
            **DABOps** helps you:
            - 📂 List all workflows in your workspace
            - 🎯 Select workflows for bundling
            - 📦 Generate asset bundles automatically
            - 💾 Save bundles to your workspace
            """)
            
            # Help section
            st.markdown("---")
            st.header("🆘 Help")
            with st.expander("How to use this app"):
                st.markdown("""
                1. **Authentication**: Ensure you're authenticated to Databricks
                2. **Discovery**: Browse and search available workflows
                3. **Selection**: Choose the workflow you want to bundle
                4. **Generation**: Generate and download the asset bundle
                5. **Storage**: Save the bundle to your workspace
                """)
            
            # Settings
            st.markdown("---")
            st.header("⚙️ Settings")
            self.render_settings()
    
    def render_settings(self):
        """Render application settings."""
        st.selectbox(
            "Bundle Format",
            ["YAML", "JSON"],
            help="Choose the output format for generated bundles"
        )
        
        st.checkbox(
            "Auto-save bundles",
            value=True,
            help="Automatically save generated bundles to workspace"
        )
        
        st.number_input(
            "Max workflows to display",
            min_value=10,
            max_value=1000,
            value=100,
            step=10,
            help="Maximum number of workflows to display at once"
        )
    
    def render_workflow_discovery(self) -> Optional[List[Dict[str, Any]]]:
        """Render workflow discovery section."""
        st.header("📂 Workflow Discovery")
        
        if not self.db_client or not self.db_client.is_authenticated():
            st.warning("Please authenticate to Databricks to discover workflows.")
            return None
        
        # Add refresh button and search
        col1, col2, col3 = st.columns([2, 2, 1])
        
        with col1:
            search_term = st.text_input(
                "🔍 Search workflows",
                placeholder="Enter workflow name, ID, or description..."
            )
        
        with col2:
            sort_by = st.selectbox(
                "Sort by",
                ["Name", "Created Date", "Last Modified", "Job ID"],
                index=2
            )
        
        with col3:
            if st.button("🔄 Refresh", help="Refresh workflow list"):
                st.cache_data.clear()
        
        # Fetch and display workflows
        try:
            with st.spinner("Discovering workflows..."):
                workflows = self.db_client.list_workflows()
            
            if not workflows:
                st.info("No workflows found in the current workspace.")
                return None
            
            # Filter workflows based on search
            if search_term:
                workflows = [
                    w for w in workflows
                    if search_term.lower() in w.get('name', '').lower() 
                    or search_term.lower() in w.get('description', '').lower()
                    or str(w.get('job_id', '')) == search_term
                ]
            
            # Sort workflows
            sort_key_map = {
                "Name": lambda w: w.get('name', '').lower(),
                "Created Date": lambda w: w.get('created_time', 0),
                "Last Modified": lambda w: w.get('modified_time', 0),
                "Job ID": lambda w: w.get('job_id', 0)
            }
            workflows.sort(key=sort_key_map[sort_by], reverse=(sort_by != "Name"))
            
            st.success(f"Found {len(workflows)} workflows")
            return workflows
            
        except Exception as e:
            handle_error(e, "Failed to discover workflows")
            return None
    
    def render_workflow_table(self, workflows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Render workflow selection table."""
        if not workflows:
            return None
        
        st.header("🎯 Workflow Selection")
        
        # Convert to DataFrame for better display
        df_data = []
        for workflow in workflows:
            df_data.append({
                'Select': False,
                'Job ID': workflow.get('job_id', 'N/A'),
                'Name': workflow.get('name', 'Unnamed'),
                'Description': workflow.get('description', 'No description')[:100] + ('...' if len(workflow.get('description', '')) > 100 else ''),
                'Created': format_job_info(workflow.get('created_time')),
                'Modified': format_job_info(workflow.get('modified_time')),
                'Status': workflow.get('status', 'Unknown')
            })
        
        df = pd.DataFrame(df_data)
        
        # Use st.data_editor for interactive selection
        edited_df = st.data_editor(
            df,
            disabled=['Job ID', 'Name', 'Description', 'Created', 'Modified', 'Status'],
            hide_index=True,
            use_container_width=True,
            height=400
        )
        
        # Find selected workflow
        selected_indices = edited_df[edited_df['Select']].index.tolist()
        
        if selected_indices:
            if len(selected_indices) > 1:
                st.warning("Please select only one workflow at a time.")
                return None
            
            selected_index = selected_indices[0]
            selected_workflow = workflows[selected_index]
            
            # Display selected workflow details
            st.success(f"Selected workflow: **{selected_workflow.get('name', 'Unnamed')}**")
            
            with st.expander("📋 Workflow Details"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Job ID:** {selected_workflow.get('job_id', 'N/A')}")
                    st.write(f"**Name:** {selected_workflow.get('name', 'Unnamed')}")
                    st.write(f"**Status:** {selected_workflow.get('status', 'Unknown')}")
                
                with col2:
                    st.write(f"**Created:** {format_job_info(selected_workflow.get('created_time'))}")
                    st.write(f"**Modified:** {format_job_info(selected_workflow.get('modified_time'))}")
                    st.write(f"**Creator:** {selected_workflow.get('creator_user_name', 'Unknown')}")
                
                if selected_workflow.get('description'):
                    st.write(f"**Description:** {selected_workflow.get('description')}")
            
            return selected_workflow
        
        return None
    
    def render_bundle_generation(self, selected_workflow: Dict[str, Any]):
        """Render bundle generation section."""
        st.header("📦 Asset Bundle Generation")
        
        if not selected_workflow:
            st.info("Please select a workflow to generate an asset bundle.")
            return
        
        workflow_name = selected_workflow.get('name', 'Unknown')
        job_id = selected_workflow.get('job_id')
        
        st.write(f"Generate asset bundle for: **{workflow_name}** (ID: {job_id})")
        
        # Bundle generation options
        col1, col2 = st.columns(2)
        
        with col1:
            bundle_name = st.text_input(
                "Bundle Name",
                value=f"{workflow_name.lower().replace(' ', '_')}_bundle",
                help="Name for the generated asset bundle"
            )
        
        with col2:
            include_dependencies = st.checkbox(
                "Include Dependencies",
                value=True,
                help="Include workflow dependencies in the bundle"
            )
        
        # Generation button
        if st.button("🚀 Generate Asset Bundle", type="primary"):
            self.generate_bundle(selected_workflow, bundle_name, include_dependencies)
    
    def generate_bundle(self, workflow: Dict[str, Any], bundle_name: str, include_dependencies: bool):
        """Generate asset bundle for the selected workflow."""
        try:
            with st.spinner("Generating asset bundle..."):
                # Generate the bundle
                bundle_content = self.bundle_generator.generate_bundle(
                    workflow,
                    bundle_name=bundle_name,
                    include_dependencies=include_dependencies
                )
                
                if bundle_content:
                    st.success("✅ Asset bundle generated successfully!")
                    
                    # Display bundle preview
                    with st.expander("📄 Bundle Preview"):
                        st.code(bundle_content, language='yaml')
                    
                    # Download and save options
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.download_button(
                            label="💾 Download Bundle",
                            data=bundle_content,
                            file_name=f"{bundle_name}.yml",
                            mime="text/yaml",
                            help="Download the bundle to your local machine"
                        )
                    
                    with col2:
                        if st.button("📂 Save to Workspace"):
                            self.save_to_workspace(bundle_content, bundle_name)
                    
                    with col3:
                        if st.button("🔄 Generate New Bundle"):
                            st.rerun()
                            
                else:
                    st.error("Failed to generate asset bundle")
                    
        except Exception as e:
            handle_error(e, "Bundle generation failed")
    
    def save_to_workspace(self, bundle_content: str, bundle_name: str):
        """Save generated bundle to workspace."""
        try:
            workspace_path = f"/Workspace/Users/{self.db_client.current_user}/DABOps/{bundle_name}.yml"
            
            success = self.db_client.save_file_to_workspace(
                content=bundle_content,
                path=workspace_path
            )
            
            if success:
                st.success(f"✅ Bundle saved to workspace: `{workspace_path}`")
            else:
                st.error("Failed to save bundle to workspace")
                
        except Exception as e:
            handle_error(e, "Failed to save to workspace")
    
    def render_footer(self):
        """Render application footer."""
        st.markdown("---")
        st.markdown(
            """
            <div style='text-align: center; color: #666;'>
                <p>🚀 DABOps - Databricks Asset Bundle Operations | 
                Built with ❤️ using Streamlit and Databricks SDK</p>
                <p>For support and documentation, visit our GitHub repository.</p>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    def run(self):
        """Run the Streamlit application."""
        try:
            # Initialize the app
            self.render_header()
            
            # Initialize clients
            if not self.initialize_clients():
                st.stop()
            
            # Render sidebar
            self.render_sidebar()
            
            # Main application flow
            workflows = self.render_workflow_discovery()
            
            if workflows:
                selected_workflow = self.render_workflow_table(workflows)
                
                if selected_workflow:
                    self.render_bundle_generation(selected_workflow)
            
            # Render footer
            self.render_footer()
            
        except Exception as e:
            logger.error(f"Application error: {str(e)}")
            st.error(f"An unexpected error occurred: {str(e)}")
            st.info("Please refresh the page or contact support if the issue persists.")

def main():
    """Main entry point for the application."""
    app = DABOpsApp()
    app.run()

if __name__ == "__main__":
    main()
