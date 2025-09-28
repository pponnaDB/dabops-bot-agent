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
        self._initialize_session_state()
    
    def _initialize_session_state(self):
        """Initialize session state for persisting data across interactions."""
        if 'generated_bundles' not in st.session_state:
            st.session_state.generated_bundles = []
        if 'generation_history' not in st.session_state:
            st.session_state.generation_history = []
        if 'last_generation_time' not in st.session_state:
            st.session_state.last_generation_time = None
        
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
            - 📂 List your workflows or all workflows in the workspace
            - 🎯 Select one or multiple workflows
            - 📦 Generate resources-only YAML files for asset bundles
            - 💾 Save separate YAML files to workspace folder: `dabops-agent-asset`
            """)
            
            # Help section
            st.markdown("---")
            st.header("🆘 Help")
            with st.expander("How to use this app"):
                st.markdown("""
                1. **Authentication**: Ensure you're authenticated to Databricks
                2. **Discovery**: Browse and search workflows (your workflows or all workflows)
                3. **Filtering**: Use the "Show workflows" filter to toggle between your workflows and all workspace workflows
                4. **Selection**: Choose one or multiple workflows using the multi-select dropdown
                5. **Generation**: Generate resources-only YAML files for each selected workflow
                6. **Storage**: Download individual files or save separate YAML files to workspace folder `dabops-agent-asset`
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
    
    def _fetch_workflows(self, user_only: bool = True) -> List[Dict[str, Any]]:
        """Cached method to fetch workflows."""
        @st.cache_data(ttl=300)  # Cache for 5 minutes
        def fetch_workflows_cached(host: str, user: str, user_only: bool):
            try:
                return self.db_client.list_workflows(user_only=user_only)
            except Exception as e:
                logger.error(f"Failed to fetch workflows: {str(e)}")
                return []
        
        if not self.db_client or not self.db_client.is_authenticated():
            return []
        
        # Create cache key from workspace info
        workspace_info = self.db_client.get_workspace_info()
        host = workspace_info.get('workspace_url', '') if workspace_info else ''
        user = self.db_client.current_user or ''
        
        return fetch_workflows_cached(host, user, user_only)

    def render_workflow_discovery(self) -> Optional[List[Dict[str, Any]]]:
        """Render workflow discovery section."""
        st.header("📂 Workflow Discovery")
        
        if not self.db_client or not self.db_client.is_authenticated():
            st.warning("Please authenticate to Databricks to discover workflows.")
            return None
        
        # Add search, sort, filter and refresh controls
        col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
        
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
            show_user_only = st.selectbox(
                "Show workflows",
                ["My workflows only", "All workflows"],
                index=0,
                help="Filter workflows by ownership"
            )
        
        with col4:
            if st.button("🔄 Refresh", help="Refresh workflow list"):
                st.cache_data.clear()
        
        # Fetch and display workflows
        try:
            with st.spinner("Discovering workflows..."):
                user_only = show_user_only == "My workflows only"
                workflows = self._fetch_workflows(user_only=user_only)
                
                # Add debug information
                if workflows:
                    logger.info(f"Successfully loaded {len(workflows)} workflows (user_only={user_only})")
                else:
                    logger.info(f"No workflows found (user_only={user_only})")
            
            if not workflows:
                if user_only:
                    st.info("No workflows found that you own. Try switching to 'All workflows' to see workflows created by others.")
                else:
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
            
            filter_text = "your workflows" if user_only else "workflows"
            st.success(f"Found {len(workflows)} {filter_text}")
            
            # Add debug info for troubleshooting
            if len(workflows) > 0:
                st.info(f"Loaded workflows: {[w.get('name', 'Unnamed') for w in workflows[:3]]}{'...' if len(workflows) > 3 else ''}")
            
            return workflows
            
        except Exception as e:
            handle_error(e, "Failed to discover workflows")
            return None
    
    def render_workflow_selection(self, workflows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Render workflow selection interface."""
        if not workflows:
            st.warning("No workflows to display.")
            return []
        
        st.header("🎯 Workflow Selection")
        st.write(f"Select one or more workflows from {len(workflows)} available:")
        
        # Create workflow options for multiselect
        workflow_options = {}
        for workflow in workflows:
            job_id = workflow.get('job_id', 'N/A')
            name = workflow.get('name', 'Unnamed')
            creator = workflow.get('creator_user_name', 'Unknown')
            display_name = f"[{job_id}] {name} (by {creator})"
            workflow_options[display_name] = workflow
        
        # Multi-select for workflows
        selected_workflow_names = st.multiselect(
            "Choose workflows:",
            options=list(workflow_options.keys()),
            help="You can select multiple workflows to generate bundles for each one"
        )
        
        selected_workflows = [workflow_options[name] for name in selected_workflow_names]
        
        if selected_workflows:
            st.success(f"Selected {len(selected_workflows)} workflow(s)")
            
            # Show selected workflows details in expandable sections
            for i, workflow in enumerate(selected_workflows):
                with st.expander(f"📋 Workflow Details: {workflow.get('name', 'Unnamed')}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Job ID:** {workflow.get('job_id', 'N/A')}")
                        st.write(f"**Name:** {workflow.get('name', 'Unnamed')}")
                        st.write(f"**Status:** {workflow.get('status', 'Unknown')}")
                    
                    with col2:
                        st.write(f"**Created:** {format_job_info(workflow.get('created_time'))}")
                        st.write(f"**Modified:** {format_job_info(workflow.get('modified_time'))}")
                        st.write(f"**Creator:** {workflow.get('creator_user_name', 'Unknown')}")
                    
                    if workflow.get('description'):
                        st.write(f"**Description:** {workflow.get('description')}")
        
        return selected_workflows
    
    def render_bundle_generation(self, selected_workflows: List[Dict[str, Any]]):
        """Render bundle generation section for multiple workflows."""
        st.header("📦 Asset Bundle Resources Generation")
        
        if not selected_workflows:
            st.info("Please select one or more workflows to generate asset bundle resources.")
            return
        
        st.info("ℹ️ This will generate only the 'resources:' section of asset bundles, with separate YAML files for each workflow.")
        
        if len(selected_workflows) == 1:
            workflow = selected_workflows[0]
            workflow_name = workflow.get('name', 'Unknown')
            job_id = workflow.get('job_id')
            st.write(f"Generate resources for: **{workflow_name}** (ID: {job_id})")
        else:
            st.write(f"Generate resources for **{len(selected_workflows)} workflows**:")
            for workflow in selected_workflows:
                st.write(f"• **{workflow.get('name', 'Unknown')}** (ID: {workflow.get('job_id')})")
        
        # Bundle generation options
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if len(selected_workflows) == 1:
                default_name = f"{selected_workflows[0].get('name', 'workflow').lower().replace(' ', '_')}_resources"
            else:
                default_name = "workflow_resources"
            
            bundle_name_prefix = st.text_input(
                "File Name Prefix",
                value=default_name,
                help="Prefix for generated resource YAML files. For multiple workflows, workflow name will be appended."
            )
        
        with col2:
            include_dependencies = st.checkbox(
                "Include Dependencies",
                value=True,
                help="Include workflow dependencies in the bundle"
            )
        
        with col3:
            auto_save_to_workspace = st.checkbox(
                "Auto-save to Workspace",
                value=True,
                help="Automatically save generated resource files to workspace folder: /Workspace/Users/{username}/dabops-agent-asset/"
            )
        
        # Previous generations info
        if st.session_state.generated_bundles:
            st.info(f"📋 {len(st.session_state.generated_bundles)} resource files from previous generations are available below.")
        
        # Generation options
        col1, col2 = st.columns(2)
        with col1:
            clear_previous = st.checkbox(
                "Clear previous generations",
                value=False,
                help="Clear previously generated resources before creating new ones"
            )
        
        with col2:
            if st.button("🗑️ Clear All Previous", help="Clear all previously generated resources"):
                st.session_state.generated_bundles = []
                st.session_state.generation_history = []
                st.rerun()
        
        # Generation buttons
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🚀 Generate Resources", type="primary"):
                self.generate_bundles(selected_workflows, bundle_name_prefix, include_dependencies, auto_save_to_workspace, clear_previous)
        
        with col2:
            if st.button("💾 Generate & Download", type="secondary"):
                self.generate_bundles(selected_workflows, bundle_name_prefix, include_dependencies, False, clear_previous, download_all=True)
    
    def generate_bundles(self, workflows: List[Dict[str, Any]], bundle_name_prefix: str, 
                        include_dependencies: bool, auto_save: bool = True, clear_previous: bool = False, download_all: bool = False):
        """Generate asset bundle resources for multiple workflows."""
        if not workflows:
            st.error("No workflows provided for resource generation.")
            return
        
        # Clear previous bundles if requested
        if clear_previous:
            st.session_state.generated_bundles = []
            st.session_state.generation_history = []
        
        generated_bundles = []
        failed_bundles = []
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        from datetime import datetime
        generation_time = datetime.now().isoformat()
        
        for i, workflow in enumerate(workflows):
            workflow_name = workflow.get('name', 'Unknown').lower().replace(' ', '_')
            if len(workflows) == 1:
                bundle_name = bundle_name_prefix
            else:
                bundle_name = f"{bundle_name_prefix}_{workflow_name}"
            
            status_text.text(f"Generating resources for {workflow.get('name', 'Unknown')}...")
            
            try:
                bundle_content = self.bundle_generator.generate_resources_only(
                    workflow,
                    include_dependencies=include_dependencies
                )
                
                if bundle_content:
                    bundle_data = {
                        'workflow': workflow,
                        'bundle_name': bundle_name,
                        'content': bundle_content,
                        'generation_time': generation_time,
                        'auto_saved': False
                    }
                    generated_bundles.append(bundle_data)
                else:
                    failed_bundles.append(workflow.get('name', 'Unknown'))
                    
            except Exception as e:
                logger.error(f"Failed to generate resources for {workflow.get('name')}: {str(e)}")
                failed_bundles.append(workflow.get('name', 'Unknown'))
            
            progress_bar.progress((i + 1) / len(workflows))
        
        # Show results
        status_text.text("Resource generation complete!")
        
        if generated_bundles:
            # Handle saving first
            if auto_save:
                for bundle_data in generated_bundles:
                    success = self.save_bundle_to_workspace(bundle_data['content'], bundle_data['bundle_name'])
                    bundle_data['auto_saved'] = success
            
            # Store in session state
            st.session_state.generated_bundles.extend(generated_bundles)
            st.session_state.generation_history.append({
                'time': generation_time,
                'count': len(generated_bundles),
                'workflows': [w.get('name', 'Unknown') for w in workflows]
            })
            st.session_state.last_generation_time = generation_time
            
            st.success(f"✅ Successfully generated {len(generated_bundles)} resource file(s)!")
            
            if failed_bundles:
                st.warning(f"⚠️ Failed to generate resources for: {', '.join(failed_bundles)}")
            
            # Handle downloading if requested
            if download_all:
                self.create_download_options(generated_bundles)
                
        else:
            st.error("❌ Failed to generate any resource files.")

    def create_download_options(self, generated_bundles: List[Dict]):
        """Create download options for newly generated bundles."""
        if len(generated_bundles) > 1:
            import zipfile
            import io
            
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                for bundle_data in generated_bundles:
                    zip_file.writestr(f"{bundle_data['bundle_name']}.yml", bundle_data['content'])
            
            st.download_button(
                label="📦 Download All Resources (ZIP)",
                data=zip_buffer.getvalue(),
                file_name="workflow_resources.zip",
                mime="application/zip",
                key=f"download_all_{len(generated_bundles)}"
            )

    def display_generated_bundles(self):
        """Display all generated bundles from session state."""
        if not st.session_state.generated_bundles:
            return
        
        st.header("📁 Generated Resource Files")
        st.write(f"Total files generated: **{len(st.session_state.generated_bundles)}**")
        
        # Group bundles by generation time for better organization
        generations = {}
        for bundle in st.session_state.generated_bundles:
            gen_time = bundle.get('generation_time', 'Unknown')
            if gen_time not in generations:
                generations[gen_time] = []
            generations[gen_time].append(bundle)
        
        # Create download all option for all bundles
        if len(st.session_state.generated_bundles) > 1:
            import zipfile
            import io
            
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
                for bundle_data in st.session_state.generated_bundles:
                    zip_file.writestr(f"{bundle_data['bundle_name']}.yml", bundle_data['content'])
            
            st.download_button(
                label="📦 Download All Generated Resources (ZIP)",
                data=zip_buffer.getvalue(),
                file_name="all_workflow_resources.zip",
                mime="application/zip",
                key="download_all_persistent"
            )
        
        # Display bundles grouped by generation
        for gen_time, bundles in generations.items():
            with st.expander(f"🕐 Generation: {gen_time[:19].replace('T', ' ')} ({len(bundles)} files)"):
                for i, bundle_data in enumerate(bundles):
                    workflow = bundle_data['workflow']
                    bundle_name = bundle_data['bundle_name']
                    
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                    
                    with col1:
                        st.write(f"**{workflow.get('name', 'Unknown')}**")
                        st.caption(f"File: {bundle_name}.yml")
                        if bundle_data.get('auto_saved'):
                            st.success("💾 Auto-saved to workspace: dabops-agent-asset")
                    
                    with col2:
                        if st.button(f"👁️ Preview", key=f"preview_{gen_time}_{i}"):
                            st.code(bundle_data['content'], language='yaml')
                    
                    with col3:
                        st.download_button(
                            label="💾 Download",
                            data=bundle_data['content'],
                            file_name=f"{bundle_name}.yml",
                            mime="text/yaml",
                            key=f"download_{gen_time}_{i}"
                        )
                    
                    with col4:
                        if not bundle_data.get('auto_saved') and st.button(f"📂 Save", key=f"save_{gen_time}_{i}"):
                            success = self.save_bundle_to_workspace(bundle_data['content'], bundle_name)
                            if success:
                                bundle_data['auto_saved'] = True
                                st.success("✅ Saved!")
                                st.rerun()
                            else:
                                st.error("❌ Failed to save")


    def save_bundle_to_workspace(self, bundle_content: str, bundle_name: str) -> bool:
        """Save generated bundle to workspace with unique naming."""
        try:
            from datetime import datetime
            
            # Create the dabops-agent-asset folder path
            folder_path = f"/Workspace/Users/{self.db_client.current_user}/dabops-agent-asset"
            base_filename = f"{bundle_name}.yml"
            workspace_path = f"{folder_path}/{base_filename}"
            
            # Check if file exists and create unique name if needed
            final_path = self._get_unique_workspace_path(workspace_path, base_filename, folder_path)
            
            return self.db_client.save_file_to_workspace(
                content=bundle_content,
                path=final_path
            )
        except Exception as e:
            logger.error(f"Failed to save bundle to workspace: {str(e)}")
            return False
    
    def _get_unique_workspace_path(self, initial_path: str, base_filename: str, folder_path: str) -> str:
        """Get a unique workspace path by appending datetime if file exists."""
        try:
            # Check if the initial file exists
            try:
                self.db_client.client.workspace.get_status(initial_path)
                # File exists, create unique name with datetime
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                name_without_ext = base_filename.rsplit('.', 1)[0]
                extension = base_filename.rsplit('.', 1)[1] if '.' in base_filename else ''
                unique_filename = f"{name_without_ext}_{timestamp}.{extension}"
                unique_path = f"{folder_path}/{unique_filename}"
                logger.info(f"File exists, using unique name: {unique_filename}")
                return unique_path
            except:
                # File doesn't exist, use original path
                return initial_path
        except Exception as e:
            logger.warning(f"Error checking file existence, using original path: {str(e)}")
            return initial_path

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
                selected_workflows = self.render_workflow_selection(workflows)
                
                if selected_workflows:
                    self.render_bundle_generation(selected_workflows)
            
            # Always display generated bundles from session state
            self.display_generated_bundles()
            
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
